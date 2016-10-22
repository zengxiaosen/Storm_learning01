package com.zengxiaosen.adalgo.etl.util;

import com.sun.tools.doclets.internal.toolkit.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * Created by Administrator on 2016/10/14.
 */
public class Tools {

    private static final String ENCODING_UTF8 = "utf-8";
    public static void openHdfsFileByRreader(org.apache.hadoop.conf.Configuration conf, String filePath, HdfsReaderRunner reader)
    throws IOException{
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fsIs = fs.open(new Path(filePath));
        BufferedReader br = new BufferedReader(new InputStreamReader(fsIs, ENCODING_UTF8));
        reader.run(br);
        br.close();
    }

    public static void openHdfsFileByGzRreader(org.apache.hadoop.conf.Configuration conf, String filePath, HdfsReaderRunner reader) throws IOException{
        FileSystem fs = FileSystem.get(conf);
        InputStream fsIs = fs.open(new Path(filePath));
        GZIPInputStream gz = new GZIPInputStream(fsIs);
        BufferedReader br = new BufferedReader(new InputStreamReader(gz));
        reader.run(br);
        br.close();
    }

    public static int getPositionType(int position){
        switch (position){
            case 2: return 1;//front
            case 40: return 1;//median
            case 5: return 1;//back

            case 11:return 2;//banner
            case 12:return 2;//banner

            case 10:return 3;//corner
            case 310:return 3;//mobile
            case 4:return 3;//stop

            default: return 0;
        }
    }

    public static String getPositionCategory(int position){
        switch (position){
            case 2: return "贴片";//front
            case 40: return "贴片";//median
            case 5: return "贴片";//back

            case 11: return "Banner";//banner
            case 12: return "Banner";//banner

            case 10: return "角标";//corner
            case 310:return "移动全屏";//mobile
            case 4:return "暂停";//stop

            default:return "null";
        }
    }

}
