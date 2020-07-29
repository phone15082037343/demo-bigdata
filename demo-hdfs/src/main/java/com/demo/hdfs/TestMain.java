package com.demo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestMain {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        Configuration conf = new Configuration();
//        conf.addResource(Objects.requireNonNull(TestMain.class.getClassLoader().getResourceAsStream("conf/core-site.xml")));
//        conf.addResource(Objects.requireNonNull(TestMain.class.getClassLoader().getResourceAsStream("conf/core-site.xml")));
//        FileSystem fs = FileSystem.get(conf);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://server:9000"), conf, "root");

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus);
        }

        fs.close();
    }

}
