package com.zengxiaosen.adalgo.etl.pos;

import com.youku.data.driver.MrJobHelper;
import com.youku.data.driver.annotation.Program;
import com.youku.data.driver.annotation.ProgramParam;
import com.zengxiaosen.adalgo.etl.util.HdfsReaderRunner;
import com.zengxiaosen.adalgo.etl.util.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by Administrator on 2016/10/19.
 */
@Program(name = "DigYesAuctionByIdeaId", description = "Join DSP Bid Log with Yes Auction Log")
public class DigYesAuctionByIdeaId {
    public static String SPLIT = "\t";
    public final  static String KEY_SPLIT = "##";
    public static final String IDEA_ID = "ideaId";
    public static final String IDEA_MAPPING = "pIdeaMapping";
    private static final String POSITION_MAPPING = "pIdMapping";

    @ProgramParam(name="pIdeaMapping", description = "idea mapping from Yes to DSP", required = true)
    private String pIdeaMapping;

    @ProgramParam(name = "pIdMapping", description = "position mapping path", required = true)
    private String pIdMapping;

    @ProgramParam(name="pIdeaId", description = "yes idea id", required = true)
    private String pIdeaId;

    public boolean run(Configuration conf, MrJobHelper jobHelper, String[] infiles, String outdir) throws IOException{
        conf.set(POSITION_MAPPING, pIdeaMapping);
        conf.set(IDEA_MAPPING, pIdeaMapping);
        conf.set(IDEA_ID, pIdeaId);

        Job job = jobHelper.newJob(conf, DSPBidMapper.class,
                DSPBidReducer.class, Text.class, Text.class, Text.class, NullWritable.class);

        //job.setNumReduceTasks(3)
        Path[] inPaths = new Path[infiles.length];
        int index = 0;
        for(String file : infiles){
            inPaths[index++] = new Path(file);
        }
        FileInputFormat.setInputPaths(job, inPaths);
        FileOutputFormat.setOutputPath(job, new Path(outdir));
        return jobHelper.runJob(job);
    }

    public static class DSPBidMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static HashMap<String, Integer> adxPidAdMap = new HashMap<String, Integer>();
        private static HashMap<String, String> ideaMap = new HashMap<String, String>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filePath = fileSplit.getPath().toString();//获取目录
            String pathName = fileSplit.getPath().getName();//获取目录名字
            int depth = fileSplit.getPath().depth();//获取目录深度
            Class<? extends FileSplit> class1 = fileSplit.getClass();//获取当前类
            long length = fileSplit.getLength();//获取文件长度
            SplitLocationInfo[] locationinfo = fileSplit.getLocationInfo();//获取位置信息
            String[] locations = fileSplit.getLocations();//获取位置
            long start = fileSplit.getStart();//the position of the first byte in the file to process
            String string = fileSplit.toString();
            if(value != null){
                try{
                    String[] valArr = value.toString().split("\t");
                    String ideaId = context.getConfiguration().get(IDEA_ID);
                    if(filePath.contains("yes/auction")){
                        if(valArr.length >= 8){
                            String[] dspInfoArr = valArr[7].split("#");
                            String bidId = valArr[4];
                            String outputKey = "";
                            String outputValue = "";
                            String isOtherDSPOK = "0";
                            ArrayList<String> statusList = new ArrayList<String>();
                            ArrayList<String> ideaList = new ArrayList<String>();
                            ArrayList<String> priceList = new ArrayList<String>();
                            ArrayList<String> otherStatusList = new ArrayList<String>();
                            String dspPos = "";
                            //dsp info
                            for(String dspInfo : dspInfoArr){
                                String[] dspEleArr = dspInfo.split("\\u005E");//转义字符
                                if(dspEleArr.length >= 4){
                                    int dspId = Integer.parseInt(dspEleArr[0]);
                                    String position = dspEleArr[2];
                                    String[] ideaArr = dspEleArr[1].split("@");
                                    if(dspId == 11167){
                                        context.getCounter(AccessDistribution.Counters.YES_BID_TOTAL_NUM).increment(1);
                                        for(String ideaInfo : ideaArr) {
                                            String[] bidInfo = ideaInfo.split(",");
                                            if(bidInfo.length > 12){
                                                priceList.add(bidInfo[0]);
                                                ideaList.add(bidInfo[5]);
                                                statusList.add(bidInfo[1]);
                                                if(bidInfo[5].equals(ideaId) && bidInfo[0].equals("451") && bidInfo[1].equals("17")){
                                                    context.write(new Text(bidId), new Text(value.toString()));
                                                    context.getCounter(Counters.YES_BID_PARSE_NUM).increment(1);
                                                    break;
                                                }
                                            }
                                        }
                                        dspPos = position;
                                    }else{
                                        int index = 0;
                                        for(String ideaInfo : ideaArr){
                                            String[] bidInfo = ideaInfo.split(",");
                                            if(bidInfo.length > 12){
                                                String otherStatus = bidInfo[1];
                                                if(otherStatusList.size() <= index){
                                                    otherStatusList.add("0");
                                                }
                                                if(otherStatus.equals("10")){
                                                    otherStatusList.set(index, "1");
                                                }
                                            }
                                            index++;
                                        }
                                    }
                                }
                            }
                            context.getCounter(Counters.YES_AUCTION_TOTAL_NUM).increment(1);
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    context.getCounter(Counters.EXCEPTION_NUM).increment(1);
                }
            }

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }
            });
            String pIdeaMapping = context.getConfiguration().get(IDEA_MAPPING);
            Tools.openHdfsFileByRreader(context.getConfiguration(), pIdeaMapping, new HdfsReaderRunner() {
                @Override
                public void run(BufferedReader br) {
                    try{
                        String line = null;
                        while((line = br.readLine()) != null){
                            String[] linesArr = line.split("\t");
                            if(linesArr.length >= 2){
                                //dsp idea id as key
                                String key = linesArr[1].trim();
                                if(!ideaMap.containsKey(key)){
                                    ideaMap.put(key, linesArr[0].trim());
                                }
                            }
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }
            });
            super.setup(context);
        }
    }

    static enum Counters{
        YES_BID_TOTAL_NUM,
        YES_BID_PARSE_NUM,
        YES_AUCTION_TOTAL_NUM,
        IDEA_BID_NUM,
        EXCEPTION_NUM,
        WIN_DEAL_NUM,
        OUTPUT_NUM,
        DSP_BID_LOG_VALID_NUM,
        DSP_BID_LOG_ERROR_NUM,
        DSP_LOG_DUP_NUM,
        DSP_IDEA_ERROR_NUM,
        YES_LOG_DUP_NUM,
        JOIN_VALID_LOG_NUM

    }
    public static class DSPBidReducer extends Reducer<Text, Text, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            while(itr.hasNext()){
                String itrStr = itr.next().toString();
                context.write(new Text(itrStr), NullWritable.get());
                context.getCounter(Counters.OUTPUT_NUM).increment(1);
            }
        }
    }

}
