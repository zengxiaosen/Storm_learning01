package com.zengxiaosen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by Administrator on 2016/11/13.
 */
public class testCreate {
    public static void main(String[] args) throws IOException {
        test1();
        test2();
    }
    static void test1() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.100.120:8020");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream dos = fs.create(new Path("/tmp/zengxiaosen/api/1.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
        bw.write("zengxiaosen");
        bw.newLine();
        bw.write("lixianshujufenxipingtai");
        bw.close();
        dos.close();
        fs.close();
    }
    static void test2() throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.120.80");
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/tmp/zengxiaosen/api/2.txt"), (short) 1)));
        bw.write("zengxiaosen");
        bw.close();
        fs.close();
    }
}
