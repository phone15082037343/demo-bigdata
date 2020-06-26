package com.demo.spark.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Properties;

public class JdbcJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("demo")
                .getOrCreate();

        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("driver", "com.mysql.jdbc.Driver");
        properties.put("fetchsize", "10000"); // read

        spark.read()
                .jdbc("jdbc:mysql://server:3306/spark?characterEncoding=utf8", "spark_gps_source", properties)
                .createOrReplaceTempView("gps_car");

        Dataset<Row> dataset = spark.sql("select * from gps_car");
        String[] columns = dataset.columns();
        System.out.println(Arrays.toString(columns));
        dataset.printSchema();
        dataset.show();

        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "123456");
        prop.put("driver", "com.mysql.jdbc.Driver");
        prop.put("batchsize", "10000"); // write
        dataset.write().
                jdbc("jdbc:mysql://server:3306/spark?characterEncoding=utf8", "spark_gps_dest", prop);

        spark.stop();
    }

}
