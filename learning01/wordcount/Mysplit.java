package wordcount;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Administrator on 2016/10/6.
 */
public class Mysplit implements IBasicBolt {

    /*
    m每个bolt最好序列化一下，免得开高并发的时候出错！
     */

    private static final long serialVersionUID = 1L;

    String patton;
    public Mysplit(String patton){
        this.patton=patton;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        try{
            String sen = tuple.getString(0);
            if(sen != null){
                for(String word : sen.split(patton)){
                    //把每个单词转化成list发送过去
                    basicOutputCollector.emit(new Values(word));
                }

            }
        }catch (Exception e){
            throw new FailedException("split fail!");
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
