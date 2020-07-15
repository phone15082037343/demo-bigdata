package com.demo.spark.job;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class JavaDirectKafkaWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
//                .setMaster("local[*]")
//                .setAppName("JavaDirectKafkaWordCount")
                ;
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
        Set<String> topicsSet = new HashSet<>(Collections.singletonList("gps_car"));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // carnumber,direction,speed,gpstime,lon,lat,carstate

//        messages.mapToPair(record -> {
//            String value = record.value();
//            String[] splits = value.split(",");
//            if (splits.length > 0) {
//                return new Tuple2<>(splits[0], 1L);
//            }
//            return new Tuple2<>(null, 0L);
//        }).reduceByKey(Long::sum).print();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("carnumber", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("direction", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("speed", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("gpstime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("lon", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("lat", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("carstate", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        messages.map(record -> {
            String value = record.value();
            String[] splits = value.split(",");
            Object[] objects = new Object[splits.length];
            for (int i = 0, len = splits.length; i < len; i++) {
                objects[i] = splits[i];
            }
            return RowFactory.create(objects);
        }).foreachRDD((rdd, time) -> {
            System.out.println("========= " + time + "=========");
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().conf());
            spark.createDataFrame(rdd, schema).createOrReplaceTempView("gps_car");
            Dataset<Row> dataset = spark.sql("select carstate, count(carstate) as total from gps_car group by carstate");
            dataset.printSchema();
            dataset.show();
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

}

class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession.builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}
