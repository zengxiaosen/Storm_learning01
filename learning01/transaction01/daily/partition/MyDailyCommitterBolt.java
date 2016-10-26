package transaction01.daily.partition;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/24.
 */
public class MyDailyCommitterBolt extends BaseTransactionalBolt implements ICommitter{
        /*
    把我们最终的数据进行汇总
     */

    //实现序列化
    private static final long serialVersionUID = 1L;
    public static final String GLOBAL_KEY = "GLOBAL_KEY";
    //我们用这个map代表数据库，最终结果还是要写到这个map里边
    public static Map<String, DbValue> dbMap = new HashMap<String, DbValue>();
    TransactionAttempt id;
    BatchOutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {
        this.id = transactionAttempt;
        this.collector = batchOutputCollector;
    }

    Map<String, Integer> countMap = new HashMap<String, Integer>();
    String  today = null;
    @Override
    public void execute(Tuple tuple) {
        //第二列

        today = tuple.getString(1);
        Integer  count = tuple.getInteger(2);
        id = (TransactionAttempt)tuple.getValue(0);
        if(today != null && count != null){
            Integer bachCount = countMap.get(today);
            if(bachCount == null){
                bachCount = 0;
            }
            bachCount += count;
            countMap.put(today, bachCount);
        }

    }

    @Override
    public void finishBatch() {
        if(countMap.size() > 0){
            DbValue value = dbMap.get(GLOBAL_KEY);
            DbValue newValue;
            if(value == null || !value.txid.equals(id.getTransactionId())){
                //更新数据库
                newValue = new DbValue();
                newValue.txid = id.getTransactionId();
                newValue.dateStr = today;
                if(value == null){
                    newValue.count = countMap.get(today);
                }else {
                    newValue.count = value.count + countMap.get("2014-01-07");
                }
                dbMap.put(GLOBAL_KEY, newValue);
            }else{
                newValue = value;
            }
        }

        System.out.println("total========================:"+dbMap.get(GLOBAL_KEY).count);
        //collector.emit(tuple)
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //如果需要往下一级发的话，这个函数里边还是需要定义下类型的

    }

    public static class DbValue{
        BigInteger txid;
        int count = 0;
        String dateStr;
    }


}
