package com.zengxiaosen.adalgo.etl.pos;


import com.zengxiaosen.adalgo.etl.util.Tools;
import com.zengxiaosen.adalgo.etl.util.HdfsReaderRunner;

import com.youku.data.driver.MrJobHelper;
import com.youku.data.driver.annotation.Program;
import com.youku.data.driver.annotation.ProgramParam;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;
/**
 * Created by Administrator on 2016/10/13.
 */
@Program(name = "AdPositionMismatch", description = "调查新的物理广告位")
public class AdPositionMismatch {
    private static String CLICK = "1";
    private static String SHOW = "0";
    private static String SPLIT = "\t";
    private static final String POSITION_MAPPING = "pIdMapping";

    @ProgramParam(name="pIdMapping", description = "position mapping path", required = true)
    private String pIdMapping;

    public boolean run(Configuration conf, MrJobHelper jobHelper, String[] infiles, String outdir) throws IOException{
        conf.set(POSITION_MAPPING, pIdMapping);

        Job job = jobHelper.newJob(conf, IdeaDistributionMapper.class, IdeaDistributionReducer.class, Text.class, Text.class, Text.class, NullWritable.class);
        job.setNumReduceTasks(1);
        Path[] inPaths = new Path[infiles.length];
        int index = 0;
        for(String file : infiles){
            inPaths[index++] = new Path(file);
        }

        FileInputFormat.setInputPaths(job, inPaths);
        FileOutputFormat.setOutputPath(job, new Path(outdir));
        return jobHelper.runJob(job);



    }

    private static class IdeaDistributionMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static String imei_regx = "\\d{15}";//全为数字
        private static String idfa_regx = "[0-9A-Za-z]{8}-[0-9A-Za-z]{4}-[0-9A-Za-z]{4}-[0-9A-Za-z]{4}-[0-9A-Za-z]{12}";
        private static String mac_regx = "[0-9A-Z]{2}:[0-9A-Z]{2}:[0-9A-Z]{2}:[0-9A-Z]{2}:[0-9A-Z]{2}:[0-9A-Z]{2}";
        private static String pc_regx = "\\d{13}\\w{3}";//\\w表示任意的字母或数字
        static HashMap<String, String> ideaData = new HashMap<String, String>();
        static HashSet<Integer> positionSet = new HashSet<Integer>(Arrays.asList(3, 4, 5, 6, 9, 10, 11, 12, 14, 17, 23, 24, 46, 47, 49, 50, 51, 52, 60, 74, 75, 95, 900));
        private static HashMap<String, Integer> adxPidAdMap = new HashMap<String, Integer>();

       @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
           String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
           if(value != null){
               try{
                   String[] valArr = value.toString().split("\t");
                   String cookie = valArr[5];
                   String requestId = valArr[22];
                   String impId = valArr[23];
                   String adPosition = valArr[24];//竞价广告位id
                   String ideaId = "";
                   String costType = "";
                   boolean isGood = false;
                   int positionType = Tools.getPositionType(adxPidAdMap.containsKey(adPosition) ? adxPidAdMap.get(adPosition) : -1);
                   if(filePath.contains("click")){
                       ideaId = valArr[33];
                       costType = valArr[29].trim();
                       //cpc + cpm
                       isGood = valArr.length >= 42 && (costType.equals("0") || costType.equals("1")) && (valArr[27].trim().equals("0") || valArr[27].trim().equals("2"));

                   }else if(filePath.contains("show")){
                       //cpc + cpm
                       ideaId = valArr[32];
                       costType = valArr[28].trim();
                       isGood = valArr.length >=42 && (costType.equals("0") || costType.equals("1")) && (valArr[41].equals("0") || valArr[41].equals("2"));
                   }
                   if(isGood && StringUtils.isNotEmpty(cookie) && StringUtils.isNotEmpty(requestId) && StringUtils.isNotEmpty(impId) && StringUtils.isNotEmpty(adPosition)){
                       isGood = Pattern.matches(pc_regx, cookie) || Pattern.matches(imei_regx, cookie)
                               || Pattern.matches(idfa_regx, cookie) || Pattern.matches(mac_regx, cookie);
                   }
                   if(isGood && positionType == 0){
                       String outputKey = ideaId + "_" + positionType + "_" + costType;
                       StringBuffer sb = new StringBuffer();
                       if (filePath.contains("click")) {
                           sb.append(CLICK);
                           context.getCounter(Counters.CLICK_NUM).increment(1L);
                       } else if (filePath.contains("show")) {
                           sb.append(SHOW);
                           context.getCounter(Counters.SHOW_NUM).increment(1L);
                       }
                       sb.append(SPLIT).append(adPosition);
                       context.write(new Text(outputKey), new Text(sb.toString()));
                   }


               }catch (Exception e){
                    e.printStackTrace();
               }
           }
       }

       protected void setup(Mapper.Context context) throws IOException, InterruptedException{
           String pIdMapping = context.getConfiguration().get(POSITION_MAPPING);
           Tools.openHdfsFileByRreader(context.getConfiguration(), pIdMapping, new HdfsReaderRunner() {
               @Override
               public void run(BufferedReader br) {
                   try{
                       String line = null;
                       while((line = br.readLine()) != null){
                           String[] linesArr = line.split(",");
                           if(linesArr.length >= 5){
                               String key = linesArr[0].trim();
                               if(!adxPidAdMap.containsKey(key)){
                                   adxPidAdMap.put(key, Integer.parseInt(linesArr[2].trim()));
                               }
                           }
                       }
                   }catch (Exception e){
                       e.printStackTrace();
                   }
               }
           });
       }

    }

    static enum Counters{
        CLICK_NUM,
        SHOW_NUM,
        TOTAL_NUM,
        OUTPUT_NUM
    }
    private static class IdeaDistributionReducer extends Reducer<Text, Text, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

}
