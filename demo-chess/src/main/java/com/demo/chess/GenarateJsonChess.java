package com.demo.chess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.demo.chess.conf.ChessArgs;
import com.demo.chess.module.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 棋子数据处理
 */
public class GenarateJsonChess {

    public static void main(String[] args) throws Exception {
        // 载入参数
        ChessArgs chessArgs = ChessArgs.load(args);
        System.out.println(chessArgs);

        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("demo")
                .getOrCreate();

        // 载入种族和职业
        loadJobRace(spark, Objects.requireNonNull(chessArgs.getJobPath())
                , Objects.requireNonNull(chessArgs.getRacePath()));
        // 载入棋子
        loadChess(spark, Objects.requireNonNull(chessArgs.getChessPath()));
        // 处理关系
        handleChess(spark);

//        Dataset<Row> dataset = spark.sql("select chessId, title, displayName, price, level from chess_final");
//        System.out.println(Arrays.toString(dataset.columns()));
//        dataset.printSchema();
//        dataset.show(10);


        Dataset<Row> dataset = spark.sql("select chessId, title, displayName, price, level from chess_final");
        System.out.println(Arrays.toString(dataset.columns()));
        dataset.printSchema();
        dataset.show(10);
        // 输出
        dataset.as(Encoders.bean(ChessFinal.class))
                .toJavaRDD()
                .map(JSON::toJSONString)
                .coalesce(1)
                .saveAsTextFile(Objects.requireNonNull(chessArgs.getChessOutput()));
        spark.stop();
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

        spark.createDataFrame(jobJavaRDD.union(raceJavaRDD), JobRace.class)
                .createOrReplaceTempView("job_race");
    }

    /**
     * 载入棋子
     */
    private static void loadChess(SparkSession spark, String chesspath) {
        JavaRDD<Chess> javaRDD = spark.read()
                .textFile(chesspath)
                .toJavaRDD()
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);

                    List<Integer> ids = new LinkedList<>();
                    String[] jobIds = jsonObject.getString("jobIds").split(",");
                    for (String jobId : jobIds) {
                        ids.add(Integer.parseInt(jobId));
                    }
                    String[] raceIds = jsonObject.getString("raceIds").split(",");
                    for (String raceId : raceIds) {
                        ids.add(Integer.parseInt(raceId) + 100);
                    }

                    return Chess.builder()
                            .chessId(Integer.parseInt(jsonObject.getString("chessId")))
                            .title(jsonObject.getString("title"))
                            .displayName(jsonObject.getString("displayName"))
                            .jobRaceIds(ids)
                            .price(Integer.parseInt(jsonObject.getString("price")))
                            .build();
                });

        spark.createDataFrame(javaRDD, Chess.class).createOrReplaceTempView("chess");

    }


    /**
     * 处理棋子关系
     */
    private static void handleChess(SparkSession spark) {
        JavaRDD<ChessSplit> javaRDD = spark.sql("select chessId, title, displayName, price, jobRaceIds from chess")
                .as(Encoders.bean(Chess.class))
                .flatMap(chess -> {
                    ChessSplit prototype = ChessSplit.builder()
                            .chessId(chess.getChessId())
                            .title(chess.getTitle())
                            .displayName(chess.getDisplayName())
                            .price(chess.getPrice())
                            .build();

                    List<ChessSplit> list = new ArrayList<>();
                    List<Integer> jobRaceIds = chess.getJobRaceIds();
                    for (Integer jobRaceId : jobRaceIds) {
                        ChessSplit chessSplit = prototype.clone();
                        chessSplit.setJobRaceId(jobRaceId);
                        list.add(chessSplit);
                    }
                    return list.iterator();
                }, Encoders.bean(ChessSplit.class))
                .toJavaRDD();

        spark.createDataFrame(javaRDD, ChessSplit.class).createOrReplaceTempView("chess_split");

        // 联查
        JavaRDD<ChessFinal> finalJavaRDD = spark.sql("select t1.chessId as chessId" +
                ", t1.title as title" +
                ", t1.displayName as displayName" +
                ", t1.price as price" +
                ", t1.jobRaceId as jobRaceId" +
                ", t2.name as name" +
                ", t2.level as level" +
                " from chess_split t1" +
                " left join job_race t2" +
                " on t1.jobRaceId = t2.jobRaceId")
                .as(Encoders.bean(ChessJobRace.class))
                .toJavaRDD()
                .mapToPair(chessJobRace -> new Tuple2<>(chessJobRace.getChessId(), chessJobRace))
                .groupByKey()
                .map(tuple2 -> {
                    Iterable<ChessJobRace> iterable = tuple2._2();
                    Iterator<ChessJobRace> iterator = iterable.iterator();
                    ChessJobRace chessJobRace = iterator.next();
                    ChessFinal chessFinal = ChessFinal.builder()
                            .chessId(chessJobRace.getChessId())
                            .title(chessJobRace.getTitle())
                            .displayName(chessJobRace.getDisplayName())
                            .price(chessJobRace.getPrice())
                            .build();

                    Map<String, List<Integer>> level = new LinkedHashMap<>();
                    level.put(chessJobRace.getName(), chessJobRace.getLevel());

                    while (iterator.hasNext()) {
                        ChessJobRace temp = iterator.next();
                        level.put(temp.getName(), temp.getLevel());
                    }
                    chessFinal.setLevel(level);
                    return chessFinal;
                });

        spark.createDataFrame(finalJavaRDD, ChessFinal.class).createOrReplaceTempView("chess_final");

    }

//    /**
//     * 注册函数
//     */
//    private static void register(SparkSession spark) {
//        spark.udf().register("map_containskey", (UDF2<scala.collection.Map<String, Seq<Integer>>, String, Boolean>) (map, value) -> {
//            if (map != null) {
//                scala.collection.Iterator<String> iterator = map.keys().iterator();
//                while (iterator.hasNext()) {
//                    String key = iterator.next();
//                    if (key.equals(value)) {
//                        return true;
//                    }
//                }
//                return false;
//            }
//            return null;
//        }, DataTypes.BooleanType);
//
//        spark.udf().register("map_size", (UDF1<scala.collection.Map, Integer>) map -> {
//            if (map != null) {
//                return map.size();
//            }
//            return null;
//        }, DataTypes.IntegerType);
//
////        spark.udf().register("map_")
//    }

//    /**
//     * 组合
//     */
//    private static void combine(ChessFinal[] input, ChessFinal[] output, int index, int start, List<List<ChessFinal>> result) {
//        if (index == output.length) {//产生一个组合序列
//            result.add(new LinkedList<>(Arrays.asList(output)));
//        } else {
//            for (int j = start; j < input.length; j++) {
//                output[index] = input[j];//记录选取的元素
//                combine(input, output, index + 1, j + 1, result);// 选取下一个元素，可选下标区间为[j+1, input.length]
//            }
//        }
//    }

//    /**
//     * 组合
//     * @param name 羁绊名称
//     * @param necessary 是否是必要的
//     * @param total 总人口数
//     * @param num 必要的人口数
//     */
//    private static List<List<ChessFinal>> combine(SparkSession spark, String name, boolean necessary
//            , int total, int num) {
//        String sql = "select * from chess_final where " + (necessary ? "" : "!") + "map_containskey(level, '" + name + "')";
//        System.out.println(sql);
//        List<ChessFinal> chessFinalList = spark.sql(sql)
//                .as(Encoders.bean(ChessFinal.class))
//                .collectAsList();
//
//        ChessFinal[] chessFinals = new ChessFinal[chessFinalList.size()];
//        chessFinalList.toArray(chessFinals);
//        // 申明变量储存结果
//        List<List<ChessFinal>> result = new LinkedList<>();
//        combine(chessFinals, new ChessFinal[necessary ? num : total - num], 0, 0, result);
//        return result;
//    }

//    /**
//     * 必要的棋子，组合
//     * @param chessArgs 参数
//     */
//    private static void combineChess(SparkSession spark, ChessArgs chessArgs) {
//        // 查询人口数
//        Integer num = spark.sql("select * from chess_final where map_containskey(level, '" + chessArgs.getName() + "')")
//                .as(Encoders.bean(ChessFinal.class))
//                .toJavaRDD()
//                .map(chessFinal -> {
//                    Map<String, List<Integer>> level = chessFinal.getLevel();
//                    return level.get(chessArgs.getName()).get(Integer.parseInt(chessArgs.getGrade()) - 1);
//                }).first();
//        System.out.println("需要人口数:" + num);
//
//        // 必要  组合结果
//        List<List<ChessFinal>> necessary = combine(spark, chessArgs.getName(), true
//                , Integer.parseInt(chessArgs.getTotal()), Objects.requireNonNull(num));
//        // 非必要  组合结果
//        List<List<ChessFinal>> unnecessary = combine(spark, chessArgs.getName(), false
//                , Integer.parseInt(chessArgs.getTotal()), Objects.requireNonNull(num));
//
//        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//
//        System.out.println(sc.parallelize(necessary).cartesian(sc.parallelize(unnecessary)).count());
//        System.out.println("done");
//
//        // 笛卡尔
////        JavaRDD<Row> javaRDD = sc.parallelize(necessary)
////                .cartesian(sc.parallelize(unnecessary))
////                .map(tuple2 -> {
////                    List<ChessFinal> list = new ArrayList<>(tuple2._1());
////                    list.addAll(tuple2._2());
////
////                    // key:羁绊名称,value:需要的羁绊数
////                    Map<String, List<Integer>> jobRaceLevel = new LinkedHashMap<>();
////                    // key:羁绊名称,数量
////                    Map<String, Integer> chessLevel = new LinkedHashMap<>();
////                    // 英雄名称集合
////                    List<String> nameList = new ArrayList<>();
////
////                    list.forEach(chessFinal -> {
////                        nameList.add(chessFinal.getDisplayName() + "(" + chessFinal.getPrice() + ")");
////
////                        Map<String, List<Integer>> level = chessFinal.getLevel();
////                        level.forEach((key, value) -> {
////                            jobRaceLevel.put(key, value);
////                            if (chessLevel.containsKey(key)) {
////                                chessLevel.put(key, chessLevel.get(key) + 1);
////                            } else {
////                                chessLevel.put(key, 1);
////                            }
////                        });
////                    });
////
////                    // 已凑成的羁绊
////                    Map<String, Integer> success = new HashMap<>();
////                    // 未凑成的羁绊
////                    Map<String, Integer> failed = new HashMap<>();
////
////                    chessLevel.forEach((key, value) -> {
////                        LinkedList<Integer> linkedList = new LinkedList<>(jobRaceLevel.get(key));
////                        if (value >= linkedList.getFirst()) {
////                            success.put(key, value);
////                        } else {
////                            failed.put(key, value);
////                        }
////                    });
////
////                    // 总价格
////                    Integer sumPrice = list.stream().map(ChessFinal::getPrice).reduce(Integer::sum).orElse(0);
////
////                    return RowFactory.create(scala.collection.JavaConversions.asScalaBuffer(nameList).toList()
////                            , scala.collection.JavaConversions.mapAsScalaMap(success)
////                            , sumPrice
////                            , scala.collection.JavaConversions.mapAsScalaMap(failed));
////                });
////
////        System.out.println(javaRDD.count());
//
////        List<StructField> fields = new ArrayList<>();
////        fields.add(DataTypes.createStructField("chesses", DataTypes.createArrayType(DataTypes.StringType), true));
////        fields.add(DataTypes.createStructField("success", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true));
////        fields.add(DataTypes.createStructField("price", DataTypes.IntegerType, true));
////        fields.add(DataTypes.createStructField("failed", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true));
////        StructType schema = DataTypes.createStructType(fields);
////
////        spark.createDataFrame(javaRDD, schema).createOrReplaceTempView("chess_combine");
//
//        // 必要的棋子
////        List<ChessFinal> necessary = spark.sql("select * from chess_final where map_containskey(level, '" + chessArgs.getName() + "')")
////                .as(Encoders.bean(ChessFinal.class))
////                .toJavaRDD()
////                .collect();
////        System.out.println(necessary);
////        ChessFinal[] unnecessaryChessFinal = new ChessFinal[necessary.size()];
////        necessary.toArray(unnecessaryChessFinal);
////        // 申明变量储存结果
////        List<List<ChessFinal>> necessaryResult = new LinkedList<>();
////        combine(unnecessaryChessFinal, new ChessFinal[num], 0, 0, necessaryResult);
////
////        // 其他棋子
////        List<ChessFinal> unnecessary = spark.sql("select * from chess_final where !map_containskey(level, '法师')")
////                .as(Encoders.bean(ChessFinal.class))
////                .toJavaRDD()
////                .collect();
////        ChessFinal[] ids = new ChessFinal[unnecessary.size()];
////        unnecessary.toArray(ids);
////        // 申明变量储存结果
////        List<List<ChessFinal>> unnecessaryResult = new LinkedList<>();
////        combine(ids, new ChessFinal[num], 0, 0, unnecessaryResult);
////
////        System.out.println(unnecessaryResult.size());
//    }


}
