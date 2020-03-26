package com.demo.geospark;

import com.mysql.jdbc.Driver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;

import java.util.Arrays;
import java.util.Properties;

public class HandleData {

    public static void main(String[] args) {

        // 1.区域面数据
        String regionpath = args.length > 0 ? args[0] : "file:///C:\\Users\\Administrator\\Desktop\\data\\handle\\region_data";
        // 2.用户
        String user = args.length > 1 ? args[1] : "postgres";
        // 3.密码
        String password = args.length > 2 ? args[2] : "123456";
        // 4.驱动
        String driver = args.length > 3 ? args[3] : "org.postgresql.Driver";
        // 5.url
        String url = args.length > 4 ? args[4] : "jdbc:postgresql://127.0.0.1:5432/spark";
        // 6.表名
        String tableName = args.length > 5 ? args[5] : "enterprise_match_less";
        // 7.输出路径
        String outputpath = args.length > 6 ? args[6] : "file:///C:\\Users\\Administrator\\Desktop\\data\\handle\\result_data0003";
        // 8.停留时间,毫秒,0不停留
        String pauseTime = args.length > 7 ? args[7] : "0";

        System.out.println("--------------------------------------------------------");
        System.out.println("regionpath:" + regionpath);
        System.out.println("user:" + user);
        System.out.println("password:" + password);
        System.out.println("driver:" + driver);
        System.out.println("url:" + url);
        System.out.println("tableName:" + tableName);
        System.out.println("outputpath:" + outputpath);
        System.out.println("--------------------------------------------------------");

        // 暂停时间
        long pause = Long.parseLong(pauseTime);
        if (pause > 0) {
            try {
                Thread.sleep(pause);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("demo")
                .getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark.sqlContext());

        // 载入区县空间数据
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("seq", ",")
                .csv(regionpath)
                .createOrReplaceTempView("region_data_temp");

        Dataset<Row> regionDataset = spark.sql("select ST_GeomFromWKT(Geometry) as Geometry, QXDM, QXMC from region_data_temp");
        regionDataset.printSchema();
        regionDataset.show();
        regionDataset.createOrReplaceTempView("region_data");

        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", password);
        prop.put("driver", driver);
        prop.put("fetchsize", "10000"); // write
        // 企业数据
        spark.read()
                .jdbc(url, tableName, prop)
                .createOrReplaceTempView("enterprise_temp");

        Dataset<Row> dataset = spark.sql("select ST_Point(cast(lnggps as decimal(24, 21)), cast(latgps as decimal(24, 21))) as location, * from enterprise_temp");
        dataset.printSchema();
        dataset.show();
        dataset.createOrReplaceTempView("enterprise");

        spark.sql("select enterprise.*" +
                ", region_data.QXDM as QXDM" +
                ", region_data.QXMC as QXMC" +
                ", region_data.Geometry as Geometry" +
                " from region_data, enterprise" +
                " where ST_Contains(region_data.Geometry , enterprise.location)")
                .createOrReplaceTempView("enterprise_data");

        // 结果数据
        Dataset<Row> resultDataset = spark.sql("select ST_AsText(location) as location" +
                ", lgl_id, lgl_name, lgl_credit_no, lgl_reg_no, lgl_org_no, lgl_tax_no, lgl_sno, lgl_stats_no, legal_person_card_type" +
                ", establish_date, operate_start_date, operate_end_date, parent_lgl_name, approve_date, approve_file_no" +
                ", property_right, business_type, industry_no, industry_type, business_scope, legal_person_name, legal_person_card_no" +
                ", lgl_org_address, regist_capital, actual_receive_capital, currency_id, operate_status_id, logout_date, lgl_logout_reason" +
                ", economic_type_id, pripid, src_update_time, valid_flag, gldm_create_time, gldm_update_time, var_ak, url, status" +
                ", lng, lat, latgps, lnggps, precise, comprehension, mark, regcap, inda, indb, indc, indd, indn, indl, indm, admin_org_unit_no, QXDM, QXMC" +
                " from enterprise_data" +
                " where admin_org_unit_no != QXDM");
        System.out.println(Arrays.toString(resultDataset.columns()));
        resultDataset.printSchema();
        resultDataset.show();
        // 输出
        resultDataset
                .coalesce(1)
                .write()
                .option("header", "true")
                .option("sep", ",")
                .csv(outputpath);

        spark.stop();
    }

}
