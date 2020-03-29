package com.demo.chess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.demo.chess.module.Chess;
import com.demo.chess.module.ChessSplit;
import com.demo.chess.module.JobRace;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class TestMain {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("demo")
                .getOrCreate();

        String chesspath = "file:///C:\\Users\\15082\\Desktop\\New folder\\chess";
        String jobpath = "file:///C:\\Users\\15082\\Desktop\\New folder\\job";
        String racepath = "file:///C:\\Users\\15082\\Desktop\\New folder\\race";

        spark.udf().register("my_level", (UDF2<scala.collection.mutable.WrappedArray<Integer>, Integer, Integer>) (level, num) -> {
            if (level != null) {
                List<Integer> list = new ArrayList<>(scala.collection.JavaConversions.asJavaCollection(level));
                if (num == 1) {
                    if (list.size() > 0) {
                        return list.get(0);
                    }
                } else if (num == 2) {
                    if (list.size() > 1) {
                        return list.get(1);
                    }
                } else if (num == 3) {
                    if (list.size() > 2) {
                        return list.get(2);
                    }
                }
            }
            return null;
        }, DataTypes.IntegerType);

        spark.udf().register("array_len", (UDF1<WrappedArray<Integer>, Integer>) array -> {
            if (array != null) {
                return array.size();
            }
            return null;
        }, DataTypes.IntegerType);

        // 载入种族和职业
        loadJobRace(spark, jobpath, racepath);
        // 载入棋子
        loadChess(spark, chesspath);

        // 棋子拆分
        splitChess(spark);

        Dataset<Row> dataset = spark.sql("select * from job_race where array_len(level) >= 3");
        System.out.println(Arrays.toString(dataset.columns()));
        dataset.printSchema();
        dataset.show();


        spark.stop();
    }

    /**
     * 拆分棋子
     */
    private static void splitChess(SparkSession spark) {
        JavaRDD<ChessSplit> javaRDD = spark.sql("select chessId, displayName, title, jobRaceIds, price from chess").toJavaRDD()
                .map(row -> Chess.builder()
                        .chessId(row.getInt(0))
                        .displayName(row.getString(1))
                        .title(row.getString(2))
                        .jobRaceIds(row.getList(3))
                        .price(row.getInt(4))
                        .build()).flatMap(chess -> {
                    List<Integer> jobRaceIds = chess.getJobRaceIds();
                    List<ChessSplit> list = new LinkedList<>();
                    ChessSplit chessSplit = ChessSplit.builder()
                            .chessId(chess.getChessId())
                            .displayName(chess.getDisplayName())
                            .title(chess.getTitle())
                            .price(chess.getPrice())
                            .build();

                    for (Integer jobRaceId : jobRaceIds) {
                        ChessSplit colneChess = chessSplit.clone();
                        colneChess.setJobRaceId(jobRaceId);
                        list.add(colneChess);
                    }
                    return list.iterator();
                });

        spark.createDataFrame(javaRDD, ChessSplit.class).createOrReplaceTempView("chess_split");
    }


    /**
     * 载入旗子
     */
    private static void loadChess(SparkSession spark, String chesspath) {
        JavaRDD<Row> javaRDD = spark.read()
                .textFile(chesspath)
                .toJavaRDD()
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);

                    List<Object> list = new LinkedList<>();
                    list.add(Integer.parseInt(jsonObject.getString("chessId")));
                    list.add(jsonObject.getString("displayName"));
                    list.add(jsonObject.getString("title"));

                    List<Integer> ids = new LinkedList<>();
                    String[] jobIds = jsonObject.getString("jobIds").split(",");
                    for (String jobId : jobIds) {
                        ids.add(Integer.parseInt(jobId));
                    }
                    String[] raceIds = jsonObject.getString("raceIds").split(",");
                    for (String raceId : raceIds) {
                        ids.add(Integer.parseInt(raceId));
                    }

                    list.add(scala.collection.JavaConversions.asScalaBuffer(ids).toList());
                    list.add(Integer.parseInt(jsonObject.getString("price")));

                    Object[] objects = new Object[list.size()];
                    list.toArray(objects);
                    return RowFactory.create(objects);
                });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("chessId", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("displayName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("title", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("jobRaceIds", DataTypes.createArrayType(DataTypes.IntegerType), true));
        fields.add(DataTypes.createStructField("price", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        spark.createDataFrame(javaRDD, schema).createOrReplaceTempView("chess");
    }

    /**
     * 载入job和race
     */
    private static void loadJobRace(SparkSession spark, String jobpath, String racepath) {
       JavaRDD<JobRace> jobJavaRDD = spark.read()
               .textFile(jobpath)
               .toJavaRDD()
               .map(json -> {
                   JSONObject jsonObject = JSON.parseObject(json);
                   JSONObject level = jsonObject.getJSONObject("level");
                   List<Integer> levelList = level.keySet()
                           .stream()
                           .map(Integer::parseInt)
                           .collect(Collectors.toList());

                   return JobRace.builder()
                           .jobRaceId(Integer.parseInt(jsonObject.getString("jobId")))
                           .name(jsonObject.getString("name"))
                           .level(levelList)
                           .build();
               });

        JavaRDD<JobRace> raceJavaRDD = spark.read()
                .textFile(racepath)
                .toJavaRDD()
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);
                    JSONObject level = jsonObject.getJSONObject("level");
                    List<Integer> levelList = level.keySet()
                            .stream()
                            .map(Integer::parseInt)
                            .collect(Collectors.toList());

                    return JobRace.builder()
                            .jobRaceId(Integer.parseInt(jsonObject.getString("raceId")))
                            .name(jsonObject.getString("name"))
                            .level(levelList)
                            .build();
                });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("jobRaceId", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("level", DataTypes.createArrayType(DataTypes.IntegerType), true));
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> javaRDD = jobJavaRDD.union(raceJavaRDD).map(jobRace -> {
            Object[] objects = new Object[3];
            objects[0] = jobRace.getJobRaceId();
            objects[1] = jobRace.getName();

            List<Integer> level = jobRace.getLevel();
            objects[2] = scala.collection.JavaConversions.asScalaBuffer(level).toList();
            return RowFactory.create(objects);
        });

        spark.createDataFrame(javaRDD, schema).createOrReplaceTempView("job_race");

    }

}
