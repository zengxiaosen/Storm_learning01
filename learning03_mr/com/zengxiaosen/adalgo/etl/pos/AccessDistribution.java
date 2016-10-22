package com.zengxiaosen.adalgo.etl.pos;

import com.youku.data.driver.MrJobHelper;
import com.youku.data.driver.annotation.Program;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by Administrator on 2016/10/13.
 */
@Program(name="AccessDistribution", description = "Join DSP Bid Log with Yes Auction Log")
public class AccessDistribution {
    public static String SPLIT = "\t";
    public final static String KEY_SPLIT = "##";
    public static final String IDEA_ID = "ideaId";
    public static final String IDEA_MAPPING = "pIdeaMapping";
    private static final String POSITION_MAPPING = "pIdMapping";

    /*
    @ProgramParam(name="pIdeaMapping", description = "idea mapping from Yes to DSP", required = true)
    private String pIdeaMapping;

    @ProgramParam(name="pIdMapping", description="position mapping path", required=true)
    private String pIdMapping;

    @ProgramParam(name="pIdeaId", description="yes idea id", required=true)
    private String pIdeaId;
*/

    public boolean run(Configuration conf, MrJobHelper jobHelper, String[] infiles, String outdir) throws IOException{
        /*
        conf.set(BIN_ID, pBinId);
        conf.set(POSITION)MAPPING, pIdMapping);
        conf.set (IDEA_MAPPING, pIdeaMapping);
        conf.set(IDEA_ID, pIdeaId)

         */
        Job job = jobHelper.newJob(conf, DSPBidMapper.class, DSPBidReducer.class, Text.class, Text.class, Text.class, NullWritable.class);
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



    public static class DSPBidMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static HashMap<String, Integer> adxPiAdMap = new HashMap<String, Integer>();
        private static HashMap<String, String> ideaMap = new HashMap<String, String>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            FileSplit inputSplit = (FileSplit) context.getInputSplit()就是获取当前行文本所对应的文件的信息
            比如你可以：
            对应文本的路径
            Path path = inputSplit.getPath();

             */
            String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
            if(value != null){
                try{
                    String[] valArr = value.toString().split("\t");
                    if(valArr.length >= 25){
                        String provinceId = valArr[3];
                        //竞价广告位的id也就是adx广告位id
                        String adxPid = valArr[23];
                        if(filePath.contains("dsp/show")){
                            adxPid = valArr[24];
                        }
                        String[] pidList = adxPid.split(",");
                        for(String pid : pidList){
                            context.write(new Text(pid + KEY_SPLIT + provinceId), new Text("1"));
                            context.getCounter(Counters.DSP_ACCESS_NUM).increment(1);
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    context.getCounter(Counters.EXCEPTION_NUM).increment(1);
                }
            }

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
        DSP_ACCESS_NUM,
        DSP_LOG_DUP_NUM,
        DSP_IDEA_ERROR_NUM,
        JOIN_VALID_LOG_NUM

    }

    public static class DSPBidReducer extends Reducer<Text, Text, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            long pv = 0;
            while(itr.hasNext()){
                String itrStr = itr.next().toString();
                if(itrStr.equals("1")){
                    pv ++;
                }
            }
            String outputKey = key.toString().replace(KEY_SPLIT, SPLIT) + SPLIT + pv;
            context.write(new Text(outputKey), NullWritable.get());
            context.getCounter(Counters.OUTPUT_NUM).increment(1);
        }
    }
}
