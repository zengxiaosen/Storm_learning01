package transaction01;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

import java.math.BigInteger;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/24.
 */
public class MyEmitter implements ITransactionalSpout.Emitter<MyData> {
    //第一个参数transactionattempt是事务的一个标识，它是在spout发送的第一列数据，包含attemptId和txid,同一个批次如果被重发，他的事务id也就是txid还是相同的，而attempid是不同的，我们可以通过attempid来判断重发了多少次。。

    Map<Long, String> dbMap = null;
    public MyEmitter(Map<Long, String> dbMap){
        this.dbMap = dbMap;
    }
    @Override
    public void emitBatch(TransactionAttempt tx, MyData coordinatorMeta, BatchOutputCollector batchOutputCollector) {
        //元数据定义开始位置，和每批次的数量
        long beginPoint = coordinatorMeta.getBeginPoint();
        int num = coordinatorMeta.getNum();
        //一个批次里面就是有num这么多
        for(long i=beginPoint; i<num + beginPoint; i++){
            //如果等于空就不发射了，否则我们会不断发射一些空的过去
            if(dbMap.get(i) == null){
                break;
            }
            batchOutputCollector.emit(new Values(tx, dbMap.get(i)));
        }
    }

    @Override
    public void cleanupBefore(BigInteger bigInteger) {

    }

    @Override
    public void close() {

    }
}
