package transaction01;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Administrator on 2016/10/24.
 */
public class MyTransactionBolt extends BaseTransactionalBolt{
    private static final long serialVersionUID = 1L;

    Integer count = 0;
    BatchOutputCollector collector;
    TransactionAttempt tx;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {

        System.err.println("MyTransactionBolt prepare "+transactionAttempt.getTransactionId() + " attempid" + transactionAttempt.getAttemptId());
        this.collector = batchOutputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        /*
        会从emit中接过来数据进行处理，同一批次处理完了会交给finishBatch做一个统一的处理
         */

        tx = (TransactionAttempt)tuple.getValue(0);
        System.err.println("MyTransactionBolt TransactionAttemp" + tx.getTransactionId() + "  attemptid"+tx.getAttemptId());
        String log = tuple.getString(1);
        if(log != null && log.length() > 0){
            count++;
        }

    }

    @Override
    public void finishBatch() {
        //把每个事物有多少行数发过去
        collector.emit(new Values(tx, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx", "count"));

    }
}
