package visits;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/6.
 * 汇总端得到的数据：每个线程发过来一个汇总数
 */
public class PVSumBolt implements IRichBolt {


    /*
    这种irichbolt形式就是成功的时候要显性的调ack方法
    失败的时候掉fail方法
     */
    private static final long serialVersionUID = 1L;
    Map<Long, Long> counts = new HashMap<Long, Long>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }


    //static long Pv = 0;
    long Pv = 0;
    @Override
    public void execute(Tuple tuple) {
        long threadID = tuple.getLong(0);
        long pv = tuple.getLong(1);
        counts.put(threadID, pv);
        long word_sum = 0;
        //获得总数，遍历counts的values，进行sum
        Iterator<Long> i = counts.values().iterator();
        while(i.hasNext()){
            word_sum += i.next();
        }

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


        System.out.println("pv = "+ Pv * 2);


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
