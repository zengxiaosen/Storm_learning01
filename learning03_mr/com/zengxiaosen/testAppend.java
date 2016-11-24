package com.zengxiaosen;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Administrator on 2016/11/13.
 */
public class testAppend {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.100.120:8020");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/tmp/zengxiaosen/createNewFile1.txt");
        FSDataOutputStream dos = fs.append(path);
        dos.write("zengxiaosen".getBytes());
        dos.close();
        fs.close();

    }
}
