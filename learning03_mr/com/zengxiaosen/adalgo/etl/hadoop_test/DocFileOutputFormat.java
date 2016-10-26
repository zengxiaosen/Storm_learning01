package com.zengxiaosen.adalgo.etl.hadoop_test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Administrator on 2016/10/22.
 */
@SuppressWarnings({"rawtypes"})
public class DocFileOutputFormat<K extends WritableComparable, V extends Writable> extends TextOutputFormat<K, V> implements HiveOutputFormat<K, V> {

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path path, Class<? extends Writable> aClass, boolean b, Properties properties, Progressable progressable) throws IOException {
        FileSystem fs = path.getFileSystem(jobConf);
        FSDataOutputStream out = fs.create(path);

        return new DocRecordWriter(out);
    }
}
