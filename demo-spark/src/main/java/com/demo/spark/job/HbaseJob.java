package com.demo.spark.job;

import com.demo.spark.entity.Gpscar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

public class HbaseJob {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("demo")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(Objects.requireNonNull(HbaseJob.class.getClassLoader().getResourceAsStream("conf/core-site.xml")));
        conf.addResource(Objects.requireNonNull(HbaseJob.class.getClassLoader().getResourceAsStream("conf/hdfs-site.xml")));
        conf.addResource(Objects.requireNonNull(HbaseJob.class.getClassLoader().getResourceAsStream("conf/hbase-site.xml")));
        String inputTable = "gps_car_source";
        String outputTable = "gps_car_dest";
        conf.set(TableInputFormat.INPUT_TABLE, inputTable);
        conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable);

        createTable(conf, inputTable);
        createTable(conf, outputTable);

        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        JavaRDD<Gpscar> javaRDD = new JavaSparkContext(spark.sparkContext())
                .newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .map(tuple2 -> {
                    Result result = tuple2._2();
                    NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap(Bytes.toBytes("default"));
                    Map<String, String> map = new HashMap<>();
                    navigableMap.forEach((key, value) -> map.put(Bytes.toString(key), Bytes.toString(value)));

                    return Gpscar.builder()
                            .carnumber(map.get("carnumber"))
                            .direction(Double.valueOf(map.get("direction")))
                            .speed(Double.valueOf(map.get("speed")))
                            .gpstime(Timestamp.valueOf(map.get("gpstime")))
                            .longitude(Double.valueOf(map.get("longitude")))
                            .latitude(Double.valueOf(map.get("latitude")))
                            .carstate(Integer.valueOf(map.get("carstate")))
                            .build();
                });

        spark.createDataFrame(javaRDD, Gpscar.class).createOrReplaceTempView("gps_car");

        Dataset<Row> dataset = spark.sql("select * from gps_car");
        String[] columns = dataset.columns();
        System.out.println(Arrays.toString(columns));
        dataset.printSchema();
        dataset.show();

        dataset.toJavaRDD()
                .mapToPair(row -> {
                    String uuid = UUID.randomUUID().toString().replaceAll("-", "");
                    Put put = new Put(Bytes.toBytes(uuid));
                    for (int i = 0, len = columns.length; i < len; i++) {
                        Object value = row.get(i);
                        put.addColumn(Bytes.toBytes("default"), Bytes.toBytes(columns[i]), Bytes.toBytes(value == null ? "" : value.toString()));
                    }
                    return new Tuple2<>(uuid, put);
                }).saveAsNewAPIHadoopDataset(job.getConfiguration());

        spark.stop();
    }

    private static void createTable(Configuration conf, String tableName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        if (!exists) {
            admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("default")).build())
                    .build());
        }

        conn.close();
    }

}
