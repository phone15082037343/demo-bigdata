package com.demo.spark.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class HbaseJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("demo")
                .config("es.index.auto.create", "true")
                .config("es.nodes", "server:9200")
                .getOrCreate();

        spark.read()
                .format("parquet")
                .load("hdfs://server:9000/data/dest/parquet")
                .createOrReplaceTempView("gps_car");

        Dataset<Row> dataset = spark.sql("select * from gps_car");
        String[] columns = dataset.columns();
        System.out.println(Arrays.toString(columns));
        dataset.printSchema();
        dataset.show();

        spark.stop();
    }

}
