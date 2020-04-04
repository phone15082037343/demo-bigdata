package com.demo.chess;


import com.alibaba.fastjson.JSON;
import com.demo.chess.conf.ChessArgs;
import com.demo.chess.module.ChessCombine;
import com.demo.chess.module.ChessCombineOutput;
import com.demo.chess.module.ChessFinal;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.*;

/**
 * 组合
 */
public class CombineChessJob {

    public static void main(String[] args) throws Exception {
        ChessArgs chessArgs = ChessArgs.load(args);
        System.out.println(chessArgs);
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("demo")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        // 注册杉树
        register(spark);

        // 载入数据
        loadChessData(spark, Objects.requireNonNull(chessArgs.getChessPath()));

        // 组合
        combineChess(spark, chessArgs);

//        Dataset<Row> dataset = spark.sql("select * from chess_combine");
//        System.out.println(Arrays.toString(dataset.columns()));
//        dataset.printSchema();
//        dataset.show();

        Properties prop = new Properties();
        prop.put("user", "postgres");
        prop.put("password", "123456");
        prop.put("driver", "org.postgresql.Driver");
        prop.put("batchsize", "10000"); // write

        spark.sql("select * from chess_combine")
                .as(Encoders.bean(ChessCombine.class))
                .map(chessCombine -> {
                    ChessCombineOutput chessCombineOutput = ChessCombineOutput.builder()
                            .displayName(String.join(", ", chessCombine.getDisplayNames()))
                            .price(chessCombine.getPrice())
                            .build();
                    List<String> temp = new LinkedList<>();
                    Map<String, Integer> success = chessCombine.getSuccess();
                    success.forEach((key, value) -> temp.add(value + ":" + key));
                    chessCombineOutput.setSuccessNum(success.size());
                    chessCombineOutput.setSuccess(String.join(", ", temp));

                    temp.clear();
                    Map<String, Integer> faield = chessCombine.getFailed();
                    faield.forEach((key, value) -> temp.add(value + ":" + key));
                    chessCombineOutput.setFaieldNum(faield.size());
                    chessCombineOutput.setFaield(String.join(", ", temp));
                    return chessCombineOutput;
                }, Encoders.bean(ChessCombineOutput.class))
                .write().
                jdbc("jdbc:postgresql://127.0.0.1:5432/spark", "chess_cike4", prop);

        spark.stop();
    }

    /**
     * 载入棋子数据
     */
    private static void loadChessData(SparkSession spark, String chesspath) {

//        JavaRDD<Row> javaRDD = spark.read()
//                .textFile(chesspath)
//                .toJavaRDD()
//                .map(json -> {
//                    ChessFinal chessFinal = JSON.parseObject(json, ChessFinal.class);
//                    Object[] objects = new Object[5];
//                    objects[0] = chessFinal.getChessId();
//                    objects[1] = chessFinal.getTitle();
//                    objects[2] = chessFinal.getDisplayName();
//                    objects[3] = chessFinal.getPrice();
//
//                    Map<String, List<Integer>> map = chessFinal.getLevel();
//                    Map<String, scala.collection.Seq<Integer>> temp = new LinkedHashMap<>();
//                    map.forEach((key, value) -> temp.put(key, scala.collection.JavaConversions.asScalaBuffer(value)));
//                    objects[4] = scala.collection.JavaConversions.mapAsScalaMap(temp);
//                    return RowFactory.create(objects);
//                });
//
//        List<StructField> fields = new ArrayList<>();
//        fields.add(DataTypes.createStructField("chessId", DataTypes.IntegerType, true));
//        fields.add(DataTypes.createStructField("title", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("displayName", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("price", DataTypes.IntegerType, true));
//        fields.add(DataTypes.createStructField("level", DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.IntegerType)), true));
//        StructType schema = DataTypes.createStructType(fields);
//
//        spark.createDataFrame(javaRDD, schema).createOrReplaceTempView("chess_final");

        JavaRDD<ChessFinal> javaRDD = spark.read()
                .textFile(chesspath)
                .toJavaRDD()
                .map(json -> JSON.parseObject(json, ChessFinal.class));

        spark.createDataFrame(javaRDD, ChessFinal.class)
                .createOrReplaceTempView("chess_final");
    }

    /**
     * 必要的棋子，组合
     * @param chessArgs 参数
     */
    private static void combineChess(SparkSession spark, ChessArgs chessArgs) {
        // 查询人口数
        Integer num = spark.sql("select * from chess_final where map_containskey(level, '" + chessArgs.getName() + "')")
                .as(Encoders.bean(ChessFinal.class))
                .toJavaRDD()
                .map(chessFinal -> {
                    Map<String, List<Integer>> level = chessFinal.getLevel();
                    return level.get(chessArgs.getName()).get(Integer.parseInt(chessArgs.getGrade()) - 1);
                }).first();
        System.out.println("需要人口数:" + num);

        // 必要  组合结果
        List<List<ChessFinal>> necessary = combine(spark, chessArgs.getName(), true
                , Integer.parseInt(chessArgs.getTotal()), Objects.requireNonNull(num));
        // 非必要  组合结果
        List<List<ChessFinal>> unnecessary = combine(spark, chessArgs.getName(), false
                , Integer.parseInt(chessArgs.getTotal()), Objects.requireNonNull(num));

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 笛卡尔
        JavaRDD<ChessCombine> javaRDD = sc.parallelize(necessary)
                .cartesian(sc.parallelize(unnecessary))
                .map(tuple2 -> {
                    ChessCombine chessCombine = new ChessCombine();
                    // 一组结果
                    List<ChessFinal> list = new ArrayList<>(tuple2._1());
                    list.addAll(tuple2._2());

                    // key:羁绊名称,value:需要的羁绊数
                    Map<String, List<Integer>> jobRaceLevel = new LinkedHashMap<>();
                    // key:羁绊名称,数量
                    Map<String, Integer> chessLevel = new LinkedHashMap<>();
                    // 英雄名称集合
                    List<String> nameList = new ArrayList<>();

                    list.forEach(chessFinal -> {
                        // 棋子名称
                        nameList.add(chessFinal.getDisplayName() + "(" + chessFinal.getPrice() + ")");

                        Map<String, List<Integer>> level = chessFinal.getLevel();
                        level.forEach((key, value) -> {
                            jobRaceLevel.put(key, value);
                            if (chessLevel.containsKey(key)) {
                                chessLevel.put(key, chessLevel.get(key) + 1);
                            } else {
                                chessLevel.put(key, 1);
                            }
                        });
                    });

                    // 已凑成的羁绊
                    Map<String, Integer> success = new HashMap<>();
                    // 未凑成的羁绊
                    Map<String, Integer> failed = new HashMap<>();

                    chessLevel.forEach((key, value) -> {
                        LinkedList<Integer> linkedList = new LinkedList<>(jobRaceLevel.get(key));
                        if (value >= linkedList.getFirst()) {
                            success.put(key, value);
                        } else {
                            failed.put(key, value);
                        }
                    });

                    // 总价格
                    Integer sumPrice = list.stream().map(ChessFinal::getPrice).reduce(Integer::sum).orElse(0);

                    chessCombine.setDisplayNames(nameList);
                    chessCombine.setPrice(sumPrice);
                    chessCombine.setSuccess(success);
                    chessCombine.setFailed(failed);
                    return chessCombine;
                });

        spark.createDataFrame(javaRDD, ChessCombine.class).createOrReplaceTempView("chess_combine");

    }

    /**
     * 组合
     * @param name 羁绊名称
     * @param necessary 是否是必要的
     * @param total 总人口数
     * @param num 必要的人口数
     */
    private static List<List<ChessFinal>> combine(SparkSession spark, String name, boolean necessary
            , int total, int num) {
        String sql = "select * from chess_final where " + (necessary ? "" : "!") + "map_containskey(level, '" + name + "')";
        System.out.println(sql);
        List<ChessFinal> chessFinalList = spark.sql(sql)
                .as(Encoders.bean(ChessFinal.class))
                .collectAsList();

        ChessFinal[] chessFinals = new ChessFinal[chessFinalList.size()];
        chessFinalList.toArray(chessFinals);
        // 申明变量储存结果
        List<List<ChessFinal>> result = new LinkedList<>();
        combine(chessFinals, new ChessFinal[necessary ? num : total - num], 0, 0, result);
        return result;
    }

    /**
     * 组合
     */
    private static void combine(ChessFinal[] input, ChessFinal[] output, int index, int start, List<List<ChessFinal>> result) {
        if (index == output.length) {//产生一个组合序列
            result.add(new LinkedList<>(Arrays.asList(output)));
        } else {
            for (int j = start; j < input.length; j++) {
                output[index] = input[j];//记录选取的元素
                combine(input, output, index + 1, j + 1, result);// 选取下一个元素，可选下标区间为[j+1, input.length]
            }
        }
    }

    /**
     * 注册函数
     */
    private static void register(SparkSession spark) {
        spark.udf().register("map_containskey", (UDF2<scala.collection.Map<String, scala.collection.Seq<Integer>>, String, Boolean>) (map, value) -> {
            if (map != null) {
                scala.collection.Iterator<String> iterator = map.keys().iterator();
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    if (key.equals(value)) {
                        return true;
                    }
                }
                return false;
            }
            return null;
        }, DataTypes.BooleanType);

        spark.udf().register("map_size", (UDF1<scala.collection.Map, Integer>) map -> {
            if (map != null) {
                return map.size();
            }
            return null;
        }, DataTypes.IntegerType);

    }

}
