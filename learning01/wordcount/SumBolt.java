package wordcount;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/6.
 */
public class SumBolt implements IBasicBolt{



    private static final long serialVersionUID = 1L;
    Map<String, Integer> counts = new HashMap<String, Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        try{

            long word_sum = 0;//总数
            long word_count = 0;//个数，去重后
            String wordString = tuple.getString(0);
            Integer countInteger = tuple.getInteger(1);
            counts.put(wordString, countInteger);
            //获取总数，遍历counts的walues，进行sum
            Iterator<Integer> i = counts.values().iterator();
            while(i.hasNext()){
                    word_sum += i.next();
            }

            //获取word去重的个数，其实就是遍历map的keyset，取个数count

            Iterator<String> i2 = counts.keySet().iterator();
            while(i2.hasNext()){
                String oneWordString = i2.next();
                if(oneWordString != null){
                    word_count ++;
                }
            }

            System.out.println("word_sum="+word_sum+";  word_count:"+word_count);

        }catch (Exception e){
            throw new FailedException("SumBolt fail!");
        }




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
