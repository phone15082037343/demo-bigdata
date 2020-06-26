package com.demo.spark.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class TxtDemoJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("demo")
                .getOrCreate();


        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ",")
                .csv("hdfs://server:9000/data/format/gps_car_header")
                .createOrReplaceTempView("gps_car");

        Dataset<Row> dataset = spark.sql("select carnumber, direction, speed, gpstime, longitude, latitude, carstate from gps_car");

        String[] columns = dataset.columns();
        System.out.println(Arrays.toString(columns));
        dataset.printSchema();
        dataset.show();

        dataset.write()
                .format("avro")
                .save("hdfs://server:9000/data/source/avro");

        spark.stop();
    }

}
