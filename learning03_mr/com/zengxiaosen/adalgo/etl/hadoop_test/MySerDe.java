package com.zengxiaosen.adalgo.etl.hadoop_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.viewfs.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2016/10/22.
 */
public class MySerDe extends AbstractSerDe {
    //params
    private List<String> columnNames = null;
    private List<TypeInfo> columnTypes = null;
    private ObjectInspector objectInspector = null;

    //seperator
    private String nullString = null;
    private String lineSeq = null;
    private String kvSeq = null;


    @Override
    public void initialize(Configuration conf, Properties tb1) throws SerDeException {

    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return null;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return null;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return null;
    }
}
