package com.zengxiaosen.adalgo.etl.hadoop_test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by Administrator on 2016/10/22.
 */
public class DocFileInputFormat extends TextInputFormat implements JobConfigurable{
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new DocRecordReader(job, (FileSplit)genericSplit);
    }
}
