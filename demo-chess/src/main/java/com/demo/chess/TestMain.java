package com.demo.chess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.demo.chess.module.Chess;
import com.demo.chess.module.ChessJobRace;
import com.demo.chess.module.ChessSplit;
import com.demo.chess.module.JobRace;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import java.util.*;
import java.util.stream.Collectors;

public class TestMain {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("demo")
                .getOrCreate();

        String name = "法师";
        // 羁绊等级
        int grade = 3;

        Stack<String> stack = new Stack<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                stack.push(arg);
            } else  {
                String key = stack.pop();
                switch (key) {
                    case "--name":
                        name = arg;
                        break;
                    case "--grade":
                        grade = Integer.parseInt(arg);
                        break;
                    default:
                        break;
                }
            }
        }

        System.out.println("name: "+ name);
        System.out.println("grade: " + grade);


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
        // 组合
        joinJobRaceChess(spark);
        // 查询临时表
        spark.sql("select * from chess_job_race where name = '" + name + "' and array_len(level) = " + grade)
                .createOrReplaceTempView("chess_job_race_temp");

//        Dataset<Row> dataset = spark.sql("select * from job_race" +
//                " where name = '" + name + "'" +
//                " and array_len(level) = " + grade);
//        System.out.println(Arrays.toString(dataset.columns()));
//        dataset.printSchema();
//        dataset.show();

        JavaRDD<Integer> numRDD = spark.sql("select my_level(level, " + grade + ") as num from chess_job_race where name = '" + name + "' and array_len(level) = " + grade).toJavaRDD()
                .map(row -> row.getInt(0))
                .filter(Objects::nonNull);
        long count = numRDD.count();
        // 需要人口数
        Integer num;
        if (count > 0) {
            num = numRDD.first();
        } else {
            throw new NullPointerException("查询人口数为空");
        }
        System.out.println("num:" + num);

        // 必要的羁绊
        List<ChessJobRace> chessJobRaceList = spark.sql("select chessId, displayName, jobRaceId, price, title, name, level from chess_job_race_temp")
                .toJavaRDD()
                .map(row -> ChessJobRace.builder()
                        .chessId(row.getInt(0))
                        .displayName(row.getString(1))
                        .jobRaceId(row.getInt(2))
                        .price(row.getInt(3))
                        .title(row.getString(4))
                        .name(row.getString(5))
                        .level(scala.collection.JavaConversions.seqAsJavaList(row.getSeq(6)))
                        .build())
                .collect();

        ChessJobRace[] chessJobRaces = new ChessJobRace[chessJobRaceList.size()];
        chessJobRaceList.toArray(chessJobRaces);
        // 申明变量储存结果
        List<List<ChessJobRace>> result = new LinkedList<>();
        combine(chessJobRaces, new ChessJobRace[num], 0, 0, result);
//        result.forEach(System.out::println);

        // 其余棋子，组合
        List<Chess> chessList = spark.sql("select chessId, displayName, title, jobRaceIds, price from chess where chessId not in (select chessId from chess_job_race_temp)").toJavaRDD()
                .map(row -> Chess.builder()
                        .chessId(row.getInt(0))
                        .displayName(row.getString(1))
                        .title(row.getString(2))
                        .jobRaceIds(scala.collection.JavaConversions.seqAsJavaList(row.getSeq(3)))
                        .price(row.getInt(4))
                        .build())
                .collect();
        Chess[] chess = new Chess[chessList.size()];
        chessList.toArray(chess);

        // 申明变量储存结果
        List<List<Chess>> resultOther = new LinkedList<>();
        combine(chess, new Chess[8 - num], 0, 0, resultOther);

        // 笛卡尔积
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // TODO
        sc.parallelize(result)
                .cartesian(sc.parallelize(resultOther))
                .map(tuple2 -> {
                    return null;
                });



//        Dataset<Row> dataset = spark.sql("select count(*) from chess_job_race");
        Dataset<Row> dataset = spark.sql("select chessId, displayName, title, jobRaceIds, price from chess where chessId not in (select chessId from chess_job_race_temp)");
        System.out.println(Arrays.toString(dataset.columns()));
        dataset.printSchema();
        dataset.show();

        spark.stop();
    }

    private static void joinJobRaceChess(SparkSession spark) {
        spark.sql("select t1.chessId as chessId" +
                ", t1.displayName as displayName" +
                ", t1.jobRaceId as jobRaceId" +
                ", t1.price as price" +
                ", t1.title as title" +
                ", t2.name as name" +
                ", t2.level as level" +
                " from chess_split t1" +
                " left join job_race t2" +
                " on t1.jobRaceId = t2.jobRaceId")
                .createOrReplaceTempView("chess_job_race");
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
                        ids.add(Integer.parseInt(raceId) + 100);
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
                            .jobRaceId(Integer.parseInt(jsonObject.getString("raceId")) + 100)
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

    /**
     * 组合
     */
    private static void combine(ChessJobRace[] input, ChessJobRace[] output, int index, int start, List<List<ChessJobRace>> result) {
        if (index == output.length) {//产生一个组合序列
            result.add(new ArrayList<>(Arrays.asList(output)));
        } else {
            for (int j = start; j < input.length; j++) {
                output[index] = input[j];//记录选取的元素
                combine(input, output, index + 1, j + 1, result);// 选取下一个元素，可选下标区间为[j+1, input.length]
            }
        }
    }

    /**
     * 组合
     */
    private static void combine(Chess[] input, Chess[] output, int index, int start, List<List<Chess>> result) {
        if (index == output.length) {//产生一个组合序列
            result.add(new ArrayList<>(Arrays.asList(output)));
        } else {
            for (int j = start; j < input.length; j++) {
                output[index] = input[j];//记录选取的元素
                combine(input, output, index + 1, j + 1, result);// 选取下一个元素，可选下标区间为[j+1, input.length]
            }
        }
    }

}
