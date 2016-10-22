package com.zengxiaosen.adalgo.etl.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/10/13.
 */
public class CookiePair  implements WritableComparable{

    public String cookie;
    public String type;


    public CookiePair(String ok, String tp){
        cookie = ok;
        type = tp;
    }

    @Override
    public int compareTo(Object o) {
        CookiePair B = (CookiePair)o;
        int cmp = cookie.compareTo(B.cookie);
        if (cmp == 0){
            cmp = type.compareTo(B.type);
        }
        return cmp;
    }


    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, cookie);
        WritableUtils.writeString(dataOutput, type);
    }
    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        cookie = WritableUtils.readString(dataInput);
        type = WritableUtils.readString(dataInput);
    }
}
