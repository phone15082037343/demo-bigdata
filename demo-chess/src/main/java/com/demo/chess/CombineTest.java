package com.demo.chess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CombineTest {

    /**
     * 组合
     */
    private static void combine(String[] input, String[] output, int index, int start, List<List<String>> result) {
        if (index == output.length) {//产生一个组合序列
//            result.add(Arrays.asList(output));
            result.add(new ArrayList<>(Arrays.asList(output)));
//            System.out.println(Arrays.toString(output));
        } else {
            for (int j = start; j < input.length; j++) {
                output[index] = input[j];//记录选取的元素
                combine(input, output, index + 1, j + 1, result);// 选取下一个元素，可选下标区间为[j+1, input.length]
            }
        }
    }

    public static void main(String[] args) {
        List<List<String>> result = new LinkedList<>();
        String[] input = { "1", "2", "3", "4", "5", "6" };
        int n = 3;//组合长度
        String[] output = new String[n];
        combine(input, output, 0, 0, result);

        result.forEach(System.out::println);

    }

}
