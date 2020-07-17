package com.demo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Objects;

public class TestMain {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(Objects.requireNonNull(TestMain.class.getClassLoader().getResourceAsStream("conf/core-site.xml")));
        conf.addResource(Objects.requireNonNull(TestMain.class.getClassLoader().getResourceAsStream("conf/core-site.xml")));
        FileSystem fs = FileSystem.get(conf);

        fs.close();
    }

}
