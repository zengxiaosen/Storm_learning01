package visits;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Administrator on 2016/10/6.
 */
public class PVBolt1 implements IRichBolt {


    /*
    这种irichbolt形式就是成功的时候要显性的调ack方法
    失败的时候掉fail方法
     */
    private static final long serialVersionUID = 1L;
    OutputCollector collector = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        //初始化方法
        this.collector = outputCollector;
    }

    String logString = null;
    String sessionid = null;
    //static long Pv = 0;
    long Pv = 0;
    @Override
    public void execute(Tuple tuple) {
        logString = tuple.getString(0);
        sessionid = logString.split("\t")[1];
       /* *//*
        这种多线程下做计算我们还必须得有synchronized使它线程安全
        这样肯定就和单线程一样
        然而还是不够健全，因为synchronized和lock在单jvm下有效，单在多jvm下无效

         *//*
        synchronized (this){
            if(sessionid != null){
                Pv ++;
            }
        }*/

         //shuffleGrouping下，pv* Executor并发数就是统计的pv
        //因为shufflegrouping是平均分配，而我们有两个线程
        //
         if(sessionid != null){
             Pv ++;
         }

         collector.emit(new Values(Thread.currentThread().getId(),Pv));
        //System.out.println("pv = "+ Pv * 2);
        System.out.println("threadid = "+ Thread.currentThread().getId()+" : pv="+Pv);



    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        /*
        定义下输出类型
         */
        outputFieldsDeclarer.declare(new Fields("threadId", "pv"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
