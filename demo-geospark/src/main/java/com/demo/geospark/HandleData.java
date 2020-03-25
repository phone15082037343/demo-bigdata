package com.demo.geospark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.sql.Array;
import java.util.Arrays;
import java.util.Map;

public class HandleData {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("demo")
                .config("es.index.auto.create", "true")
                .config("es.nodes", "127.0.0.1:9200")
                .getOrCreate();

        // 载入区域代码
        String codepath = "file:///C:\\Users\\Administrator\\Desktop\\data\\handle\\region_code";
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("seq", ",")
                .csv(codepath)
                .createOrReplaceTempView("region_code");

//        Dataset<Row> dataset = spark.sql("select * from region_code");


        // 载入区县空间数据
        String regionpath = "file:///C:\\Users\\Administrator\\Desktop\\data\\handle\\region_data";
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("seq", ",")
                .csv(regionpath)
                .createOrReplaceTempView("region_data_temp");

        Dataset<Row> dataset = spark.sql("select * from region_data_temp");

        // 载入数据,根据空间点位追加区域代码
        JavaPairRDD<String, String> javaPairRDD = JavaEsSpark.esJsonRDD(new JavaSparkContext(spark.sparkContext()), "enterprise_demo_part1");
        javaPairRDD.take(10).forEach(System.out::println);

        System.out.println(Arrays.toString(dataset.columns()));
        dataset.printSchema();
        dataset.show();

        spark.stop();
    }

}
