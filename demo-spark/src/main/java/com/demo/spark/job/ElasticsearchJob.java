package com.demo.spark.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.Arrays;

public class ElasticsearchJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("demo")
                .config("es.index.auto.create", "true")
                .config("es.nodes", "server:9200")
                .getOrCreate();

        JavaEsSparkSQL.esDF(spark, "gps_car_source").createOrReplaceTempView("gps_car");

        Dataset<Row> dataset = spark.sql("select * from gps_car");
        String[] columns = dataset.columns();
        System.out.println(Arrays.toString(columns));
        dataset.printSchema();
        dataset.show();

        JavaEsSparkSQL.saveToEs(dataset, "gps_car_dest");

//        JavaRDD<String> jsonRDD = dataset.toJavaRDD()
//                .map(row -> {
//                    Map<String, Object> map = new LinkedHashMap<>();
//                    for (int i = 0, len = columns.length; i < len; i++) {
//                        map.put(columns[i], row.get(i));
//                    }
//                    return JSON.toJSONString(map);
//                });
//
//        JavaEsSpark.saveJsonToEs(jsonRDD, "gps_car_source");

        spark.stop();
    }

}
