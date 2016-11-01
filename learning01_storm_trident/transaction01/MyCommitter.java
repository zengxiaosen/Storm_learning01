package transaction01;

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
public class MyCommitter extends BaseTransactionalBolt implements ICommitter{
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
    int sum = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {
        this.id = transactionAttempt;
        this.collector = batchOutputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //第二列

        sum += tuple.getInteger(1);
    }

    @Override
    public void finishBatch() {
        DbValue value = dbMap.get(GLOBAL_KEY);
        DbValue newValue;
        if(value == null || !value.txid.equals(id.getTransactionId())){
            //更新数据库
            newValue = new DbValue();
            newValue.txid = id.getTransactionId();
            if(value == null){
                newValue.count = sum;
            }else {
                newValue.count = value.count + sum;
            }
            dbMap.put(GLOBAL_KEY, newValue);
        }else{
            newValue = value;
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
    }


}
