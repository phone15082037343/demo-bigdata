package com;

import java.util.Arrays;

public class CombineTest {

    /**
     * 组合
     */
    private static void combine(String[] input, String[] output, int index, int start) {
        if (index == output.length) {//产生一个组合序列
            System.out.println(Arrays.toString(output));
        } else {
            for (int j = start; j < input.length; j++) {
                output[index] = input[j];//记录选取的元素
                combine(input, output, index + 1, j + 1);// 选取下一个元素，可选下标区间为[j+1, input.length]
            }
        }
    }

    public static void main(String[] args) {
        String[] input = { "1", "2", "3", "4", "5", "6" };
        int n = 3;//组合长度
        String[] output = new String[n];
        combine(input, output, 0, 0);
    }


}
