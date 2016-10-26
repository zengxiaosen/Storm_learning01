package com.zengxiaosen.adalgo.etl.hadoop_test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.omg.CORBA.PRIVATE_MEMBER;

import java.io.IOException;

/**
 * Created by Administrator on 2016/10/22.
 */
public class DocRecordWriter implements FileSinkOperator.RecordWriter {

    private FSDataOutputStream out;
    private final String DOC_START = "<DOC>";
    private final String DOC_END = "</DOC>";


    public DocRecordWriter(FSDataOutputStream out) {
        this.out = out;
    }

    @Override
    public void write(Writable writable) throws IOException {
        write(DOC_START);
        write("\n");
        write(writable.toString());
        write("\n");
        write(DOC_END);
        write("\n");
    }

    private void write(String str) throws IOException{
        out.write(str.getBytes(), 0, str.length());
    }

    @Override
    public void close(boolean b) throws IOException {
        out.flush();
        out.close();
    }
}
