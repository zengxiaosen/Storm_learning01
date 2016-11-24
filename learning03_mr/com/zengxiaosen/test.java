package com.zengxiaosen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Administrator on 2016/11/13.
 */
public class test {
    public static void main(String[] args) throws IOException {
        test1();
        test2();
    }
    /*
    指定绝对路径
     */
    private static void test1() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.106.18.65：8020");
        FileSystem fs = FileSystem.get(conf);
        boolean created = fs.createNewFile(new Path("/tmp/zengxiaosen/createNewFile1.txt"));
        System.out.println(created ? "创建成功" : "创建失败");
        fs.close();
    }
    /*
    使用相对路径
     */
    private static void test2() throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10/106.18.65:8020");
        FileSystem fs = FileSystem.get(conf);
        boolean created = fs.createNewFile(new Path("createNewFile2.txt"));
        System.out.println(created);
        fs.close();
    }
}
