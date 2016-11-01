package transaction01.daily;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tools.DataFmt;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/25.
 */
public class MyDailyBatchBolt implements IBatchBolt<TransactionAttempt> {

    private static final long serialVersionUID = 1l;
    BatchOutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {
        this.collector = batchOutputCollector;

    }

    Map<String, Integer> countMap = new HashMap<String, Integer>();
    Integer count = null;
    String today = null;
    TransactionAttempt tx = null;
    @Override
    public void execute(Tuple tuple) {
        String log = tuple.getString(1);
        tx = (TransactionAttempt)tuple.getValue(0);
        if(log != null && log.split("\\t").length >= 3){
            today = DataFmt.getCountDate(log.split("\\t")[2], DataFmt.date_short);
            count = countMap.get(today);
            if(count == null){
                count = 0;
            }
            count ++;
            countMap.put(today, count);
        }
    }

    @Override
    public void finishBatch() {

        collector.emit(new Values(tx, today, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx","date","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
