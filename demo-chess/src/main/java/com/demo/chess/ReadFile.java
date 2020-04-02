package com.demo.chess;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ReadFile {


    public static void main(String[] args) {
        String filepath = "C:\\Users\\15082\\Desktop\\New folder\\result007\\part-00000-dae75080-910b-4a8f-b51e-1e89c2ae2f64-c000.json";

        int i = 0;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filepath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                i++;
                if (i > 100) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
