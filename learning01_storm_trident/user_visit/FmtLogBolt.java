package user_visit;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tools.DataFmt;

import java.util.Map;

/**
 * Created by Administrator on 2016/10/8.
 */
public class FmtLogBolt implements IBasicBolt{
    /*
    这个相比于irich的好处就是不用显性的去回调它的ask和fail方法

     */
    private static final long serialVersionUID = 1L;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    String eachLog = null;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        eachLog = tuple.getString(0);
        if(eachLog != null && eachLog.length() > 0){
            //日期，session_id
            basicOutputCollector.emit(new Values(DataFmt.getCountDate(eachLog.split("\t")[2],DataFmt.date_short),eachLog.split("\t")[1]));

        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //这两个名称的作用就是在下一级通过这两个名称获取
        outputFieldsDeclarer.declare(new Fields("date","session_id"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
