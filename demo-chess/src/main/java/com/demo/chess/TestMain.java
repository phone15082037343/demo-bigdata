package com.demo.chess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.demo.chess.module.*;
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
import scala.Tuple2;
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

        // 注册自定义函数
        register(spark);

        // 载入种族和职业
        loadJobRace(spark, jobpath, racepath);
        // 载入棋子
        loadChess(spark, chesspath);

        // 棋子拆分
        splitChess(spark);
        // 组合
        joinJobRaceChess(spark);
        // 组合棋子
        genarateChess(spark);

        // 创建临时表，满足条件的
        spark.sql("select * from chess_job_race where name = '" + name + "' and array_len(level) = " + grade)
                .createOrReplaceTempView("chess_job_race_temp");

        // 需要人口数
        JavaRDD<Integer> numRDD = spark.sql("select my_level(level, " + grade + ") as num from chess_job_race where name = '" + name + "' and array_len(level) = " + grade).toJavaRDD()
                .map(row -> row.getInt(0))
                .filter(Objects::nonNull);
        long count = numRDD.count();
        Integer num;
        if (count > 0) {
            num = numRDD.first();
        } else {
            throw new NullPointerException("查询人口数为空");
        }
        System.out.println("num:" + num);

        // 组合棋子，并生成结果
        combineChess(spark, num);

        // 计算最终结果, left join 并且压缩
//        joinChess(spark);

        Dataset<Row> dataset = spark.sql("select * from chess_combine");
        System.out.println(Arrays.toString(dataset.columns()));
        dataset.printSchema();
        dataset.show(100);
        spark.stop();
    }

    /**
     * 生成棋子
     */
    private static void genarateChess(SparkSession spark) {
        JavaRDD<Row> javaRDD = spark.sql("select chessId, displayName, jobRaceId, price, title, name, level from chess_job_race")
                .toJavaRDD()
                .mapToPair(row -> {
                    ChessJobRace chessJobRace = ChessJobRace.builder()
                            .chessId(row.getInt(0))
                            .displayName(row.getString(1))
                            .jobRaceId(row.getInt(2))
                            .price(row.getInt(3))
                            .title(row.getString(4))
                            .name(row.getString(5))
                            .level(scala.collection.JavaConversions.seqAsJavaList(row.getSeq(6)))
                            .build();
                    return new Tuple2<>(chessJobRace.getChessId(), chessJobRace);
                })
                .groupByKey()
                .map(tuple2 -> {
                    Iterable<ChessJobRace> iterable = tuple2._2();
                    Iterator<ChessJobRace> iterator = iterable.iterator();
                    // 第一个棋子
                    ChessJobRace chessJobRace = iterator.next();

                    ChessFinal chessFinal = ChessFinal.builder()
                            .chessId(chessJobRace.getChessId())
                            .displayName(chessJobRace.getDisplayName())
                            .title(chessJobRace.getTitle())
                            .price(chessJobRace.getPrice())
                            .build();

                    Map<String, scala.collection.Seq<Integer>> map = new LinkedHashMap<>();
                    map.put(chessJobRace.getName(), scala.collection.JavaConversions.asScalaBuffer(chessJobRace.getLevel()).toList());
                    while (iterator.hasNext()) {
                        ChessJobRace chess = iterator.next();
                        map.put(chess.getName(), scala.collection.JavaConversions.asScalaBuffer(chess.getLevel()).toList());
                    }

                    return RowFactory.create(chessFinal.getChessId()
                            , chessFinal.getDisplayName()
                            , chessFinal.getTitle()
                            , chessFinal.getPrice()
                            , scala.collection.JavaConversions.mapAsScalaMap(map));
                });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("chessId", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("displayName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("title", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("price", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("level", DataTypes.createMapType(DataTypes.StringType
                , DataTypes.createArrayType(DataTypes.IntegerType)), true));
        StructType schema = DataTypes.createStructType(fields);

        spark.createDataFrame(javaRDD, schema).createOrReplaceTempView("chess_final");

    }

    /**
     * 棋子种族主页关联
     */
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

//    /**
//     * 组合
//     */
//    private static void combine(ChessJobRace[] input, ChessJobRace[] output, int index, int start, List<List<ChessJobRace>> result) {
//        if (index == output.length) {//产生一个组合序列
//            result.add(new ArrayList<>(Arrays.asList(output)));
//        } else {
//            for (int j = start; j < input.length; j++) {
//                output[index] = input[j];//记录选取的元素
//                combine(input, output, index + 1, j + 1, result);// 选取下一个元素，可选下标区间为[j+1, input.length]
//            }
//        }
//    }

//    /**
//     * 组合
//     */
//    private static void combine(Chess[] input, Chess[] output, int index, int start, List<List<Chess>> result) {
//        if (index == output.length) {//产生一个组合序列
//            result.add(new ArrayList<>(Arrays.asList(output)));
//        } else {
//            for (int j = start; j < input.length; j++) {
//                output[index] = input[j];//记录选取的元素
//                combine(input, output, index + 1, j + 1, result);// 选取下一个元素，可选下标区间为[j+1, input.length]
//            }
//        }
//    }

    /**
     * 组合
     */
    private static <T> void combine(T[] input, T[] output, int index, int start, List<List<T>> result) {
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
     * 必要的棋子，组合
     */
    private static List<List<ChessFinal>> combineChess(SparkSession spark, int num, boolean isNecessary) {
        // 必要的羁绊，棋子ID
        List<ChessFinal> chessIdList = spark.sql("select chessId, displayName, title, price, level from chess_final where chessId" +
                (isNecessary ? "": " not") +
                " in (select chessId from chess_job_race_temp)")
                .toJavaRDD()
                .map(row -> {
                    Map<String, scala.collection.mutable.WrappedArray<Integer>> rowMap = row.getJavaMap(4);
                    Map<String, List<Integer>> level = new LinkedHashMap<>();
                    rowMap.forEach((key, value) -> level.put(key, scala.collection.JavaConversions.seqAsJavaList(value)));
                    return ChessFinal.builder()
                            .chessId(row.getInt(0))
                            .displayName(row.getString(1))
                            .title(row.getString(2))
                            .price(row.getInt(3))
                            .level(level)
                            .build();
                })
                .collect();
        ChessFinal[] ids = new ChessFinal[chessIdList.size()];
        chessIdList.toArray(ids);
        // 申明变量储存结果
        List<List<ChessFinal>> result = new LinkedList<>();
        combine(ids, new ChessFinal[isNecessary ? num : 8 - num], 0, 0, result);
        return result;
    }

    /**
     * 笛卡尔积
     */
    private static void combineChess(SparkSession spark, int num) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 羁绊计算
        JavaRDD<Row> javaRDD = sc.parallelize(combineChess(spark, num, true))
                .cartesian(sc.parallelize(combineChess(spark, num, false)))
                .map(tuple2 -> {
                    List<ChessFinal> list = new ArrayList<>(tuple2._1());
                    list.addAll(tuple2._2());

                    // key:羁绊名称,value:需要的羁绊数
                    Map<String, List<Integer>> jobRaceLevel = new LinkedHashMap<>();
                    // key:羁绊名称,数量
                    Map<String, Integer> chessLevel = new LinkedHashMap<>();
                    // 英雄名称集合
                    List<String> nameList = new ArrayList<>();

                    list.forEach(chessFinal -> {
                        nameList.add(chessFinal.getTitle() + ":" + chessFinal.getDisplayName());

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

                    // TODO 价格
                    Integer sumPrice = list.stream().map(ChessFinal::getPrice).reduce(Integer::sum).orElse(0);

                    return RowFactory.create(scala.collection.JavaConversions.asScalaBuffer(nameList).toList()
                            , scala.collection.JavaConversions.mapAsScalaMap(success)
                            , sumPrice
                            , scala.collection.JavaConversions.mapAsScalaMap(failed));
                });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("chesses", DataTypes.createArrayType(DataTypes.StringType), true));
        fields.add(DataTypes.createStructField("success", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true));
        fields.add(DataTypes.createStructField("price", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("failed", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true));
        StructType schema = DataTypes.createStructType(fields);

        spark.createDataFrame(javaRDD, schema).createOrReplaceTempView("chess_combine");

    }

    /**
     * 注册自定义函数
     */
    private static void register(SparkSession spark) {
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
    }

}
