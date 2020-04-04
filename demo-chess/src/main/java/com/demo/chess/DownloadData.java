package com.demo.chess;

//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.demo.chess.entity.Chess;
//import com.demo.chess.entity.ChessData;
//import com.demo.chess.entity.Equip;
//import com.demo.chess.entity.EquipData;
//
//import java.io.*;
//import java.net.URL;
//import java.net.URLConnection;
//import java.nio.charset.StandardCharsets;
//import java.util.List;

public class DownloadData {

    public static void main(String[] args) throws Exception{
//        String dir = "C:\\Users\\15082\\Desktop\\New folder";
//        chess(dir + File.separator + "chess");
//        job(dir + File.separator + "job");
//        race(dir + File.separator + "race");
//        equip(dir + File.separator + "equip");
    }

//    private static void chess(String dir) throws IOException {
//        mkdir(dir);
//        URL url = new URL("https://game.gtimg.cn/images/lol/act/img/tft/js/chess.js");
//        URLConnection conn = url.openConnection();
//        InputStream inputStream = conn.getInputStream();
//
//        String str = getStr(inputStream);
//        ChessData data = JSON.parseObject(str, ChessData.class);
//        List<Chess> list = data.getData();
//        System.out.println("chess:" + list.size());
//        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dir + File.separator + "chess.json"), StandardCharsets.UTF_8));
//        for (Chess chess : list) {
//            bufferedWriter.write(JSON.toJSONString(chess) + "\r\n");
//        }
//
//        bufferedWriter.close();
//    }
//
//    private static void job(String dir) throws IOException{
//        mkdir(dir);
//        URL url = new URL("https://game.gtimg.cn/images/lol/act/img/tft/js/job.js");
//        URLConnection conn = url.openConnection();
//        InputStream inputStream = conn.getInputStream();
//
//        String str = getStr(inputStream);
//        JSONObject jsonObject = JSON.parseObject(str);
//        JSONArray datas = jsonObject.getJSONArray("data");
//        System.out.println("job:" + datas.size());
//
//        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dir + File.separator + "job.json"), StandardCharsets.UTF_8));
//        for (Object data : datas) {
//            bufferedWriter.write(data.toString() + "\r\n");
//        }
//        bufferedWriter.close();
//    }
//
//    private static void race(String dir) throws IOException{
//        mkdir(dir);
//        URL url = new URL("https://game.gtimg.cn/images/lol/act/img/tft/js/race.js");
//        URLConnection conn = url.openConnection();
//        InputStream inputStream = conn.getInputStream();
//
//        String str = getStr(inputStream);
//        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dir + File.separator + "race.json"), StandardCharsets.UTF_8));
//        JSONObject jsonObject = JSON.parseObject(str);
//        JSONArray datas = jsonObject.getJSONArray("data");
//        System.out.println("race:" + datas.size());
//        for (Object data : datas) {
//            bufferedWriter.write(data.toString() + "\r\n");
//        }
//        bufferedWriter.close();
//
//    }
//
//
//    private static void equip(String dir) throws IOException{
//        mkdir(dir);
//        URL url = new URL("https://game.gtimg.cn/images/lol/act/img/tft/js/equip.js");
//        URLConnection conn = url.openConnection();
//        InputStream inputStream = conn.getInputStream();
//
//        String str = getStr(inputStream);
//        EquipData data = JSON.parseObject(str, EquipData.class);
//        List<Equip> list = data.getData();
//        System.out.println("equip:" + list.size());
//        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dir + File.separator + "equip.json"), StandardCharsets.UTF_8));
//        for (Equip equip : list) {
//            bufferedWriter.write(JSON.toJSONString(equip) + "\r\n");
//        }
//        bufferedWriter.close();
//    }
//
//
//    private static String getStr(InputStream inputStream) throws IOException {
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
//        String line;
//        StringBuilder buffer = new StringBuilder();
//        while ((line = bufferedReader.readLine()) != null) {
//            buffer.append(line);
//        }
//        return buffer.toString();
//    }
//
//    private static void mkdir(String path) {
//        File file = new File(path);
//        if (!file.exists()) {
//            file.mkdirs();
//        }
//    }

}
