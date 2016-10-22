package com.zengxiaosen.adalgo.etl.hadoop_test;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/10/22.
 */
public class TextPathFilter extends Configured implements PathFilter {
    @Override
    public boolean accept(Path path) {
        FileSystem fs;
        try{
            fs = FileSystem.get(getConf());
            FileStatus fileStatus = fs.getFileStatus(path);
            List<String> lstName = new ArrayList<String>();
            lstName.add("input1");
            lstName.add("input2");
            lstName.add("input3");
            lstName.add("input4");
            if(fileStatus.isDirectory()){
                //是目录的话返回true
                return true;
            }
            if(fileStatus.isFile() && lstName.contains(fileStatus.getPath().getParent().getName())){
                //是文件的话且满足过滤条件返回true
                return true;
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return false;
    }
    /*
    driverL类就可以写：
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));    //输入路径
    FileInputFormat.setInputDirRecursive(job, true);// 递归输入
    FileInputFormat.setInputPathFilter(job, TextPathFilter.class);   //指定pathfilter类
     */
}
