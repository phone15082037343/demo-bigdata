package com.demo.chess.conf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ChessArgs implements Serializable {

    /** 棋子 */
    private String chessPath;
    /** 种族 */
    private String jobPath;
    /** 职业 */
    private String racePath;
    /** 输出路径 */
    private String chessOutput;
    /** 种族或职业名称 */
    private String name;
    /** 该羁绊等级，如：1、2、3，需要人口：2、4、6 */
    private String grade;
    /** 总人数 */
    private String total = "8";
    /** url */
    private String url;
    /** 驱动类型 */
    private String driverClassName;
    /** 用户名 */
    private String username;
    /** 密码 */
    private String password;
    /** 表名 */
    private String tableName;


    public static ChessArgs load(String[] args) throws Exception {
        Map<String, String> map = new HashMap<>();
        Stack<String> stack = new Stack<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                stack.push(arg);
            } else {
                String key = stack.pop();
                map.put(key, arg);
            }
        }

        Class<ChessArgs> clazz = ChessArgs.class;
        ChessArgs chessArgs = clazz.newInstance();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String[] words = entry.getKey().substring(2).split("-");
            String name = Arrays.stream(words)
                    .map(ChessArgs::captureName)
                    .collect(Collectors.joining(""));
            Method method = clazz.getDeclaredMethod("set" + name, String.class);
            method.invoke(chessArgs, entry.getValue());
        }
        return chessArgs;
    }

    // 首字母大写
    private static String captureName(String name) {
        char[] cs = name.toCharArray();
        cs[0] -= 32;
        return String.valueOf(cs);
    }

}
