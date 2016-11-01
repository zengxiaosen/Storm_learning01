package user_visit;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/8.
 * 统计每个session_id的pv
 */
public class DeepVisitBolt implements IBasicBolt{
    /*

     */
    private static final long serialVersionUID = 1L;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }
    //map存局部汇总的值
    Map<String, Integer> counts = new HashMap<String, Integer>();
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String dateString = tuple.getStringByField("date");
        String session_id = tuple.getStringByField("session_id");
        /*
        我们要去重，就需要把我们要去重的东西放到map的key里面
         */
        Integer count = counts.get(dateString+"_"+session_id);
        if(count == null){
            count = 0;
        }
        count++;
        counts.put(dateString+"_"+session_id, count);
        //这是我们的局部汇总，我们需要把它发到我们的下一级做一个总的汇总
        basicOutputCollector.emit(new Values(dateString+"_"+session_id, count));




    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("date_session_id", "count"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
