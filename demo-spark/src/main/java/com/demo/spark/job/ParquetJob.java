package com.demo.spark.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class ParquetJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("demo")
                .getOrCreate();

        spark.read()
                .format("parquet")
                .load("hdfs://server:9000/data/source/parquet")
                .createOrReplaceTempView("gps_car");

        Dataset<Row> dataset = spark.sql("select * from gps_car");
        String[] columns = dataset.columns();
        System.out.println(Arrays.toString(columns));
        dataset.printSchema();
        dataset.show();

        spark.sql("select count(*) from gps_car").show();

        dataset.write()
                .format("parquet")
                .save("hdfs://server:9000/data/dest/parquet");

        spark.stop();
    }
}
