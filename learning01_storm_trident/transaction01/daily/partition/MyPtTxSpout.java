package transaction01.daily.partition;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import transaction01.MyData;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Administrator on 2016/10/25.
 */
public class MyPtTxSpout implements IPartitionedTransactionalSpout<MyData> {


    private static final long serialVersionUID = 1L;
    public static int BATCH_NUM = 10;
    public Map<Integer, Map<Long, String>> PT_DATA_MP = new HashMap<Integer, Map<Long, String>>();

    public MyPtTxSpout(){

        Random random = new Random();

        String[] hosts = {"www.taobao.com"};
        String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
                "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
        String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };

        for(int j=0; j< 5; j++){
            //造五个分区
            HashMap<Long, String> dbMap = new HashMap<Long, String>();
            for (long i = 0; i < 100; i++){
                dbMap.put(i, hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]+"\n");
            }
            PT_DATA_MP.put(j, dbMap);
        }


    }
    @Override
    public Coordinator getCoordinator(Map map, TopologyContext topologyContext) {
        return new MyCoordinator();
    }

    @Override
    public Emitter<MyData> getEmitter(Map map, TopologyContext topologyContext) {
        return new MyEmitter();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx","log"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public class MyCoordinator implements IPartitionedTransactionalSpout.Coordinator{

        @Override
        public int numPartitions() {
            return 5;
        }

        @Override
        public boolean isReady() {
            Utils.sleep(1000);
            return true;
        }

        @Override
        public void close() {

        }
    }

    public class MyEmitter  implements IPartitionedTransactionalSpout.Emitter<MyData>{

        @Override
        public MyData emitPartitionBatchNew(TransactionAttempt transactionAttempt, BatchOutputCollector batchOutputCollector, int partition, MyData myData) {
            long beginPoint = 0;
            if(myData == null){
                beginPoint = 0;
            }else{
                beginPoint = myData.getBeginPoint() + myData.getNum();
            }
            MyData mata = new MyData();
            mata.setBeginPoint(beginPoint);
            mata.setNum(BATCH_NUM);

            emitPartitionBatch(transactionAttempt, batchOutputCollector, partition, mata);
            //这个方法就是返回元数据，这个方法里面包含了事务的开始位置以及事务的数量
            System.err.println("启动一个事务："+mata.toString());
            return mata;

        }

        @Override
        public void emitPartitionBatch(TransactionAttempt transactionAttempt, BatchOutputCollector batchOutputCollector, int partition, MyData partitionMeta) {

            System.err.println("emitPartitionBatch partition:"+partition);
            //元数据定义开始位置，和每批次的数量
            long beginPoint = partitionMeta.getBeginPoint();
            int num = partitionMeta.getNum();
            Map<Long, String> batchMap = PT_DATA_MP.get(partition);
            //一个批次里面就是有num这么多
            for(long i=beginPoint; i<num + beginPoint; i++){
                //如果等于空就不发射了，否则我们会不断发射一些空的过去
                if(batchMap.get(i) == null){
                    break;
                }
                batchOutputCollector.emit(new Values(transactionAttempt, batchMap.get(i)));
            }
        }

        @Override
        public void close() {

        }
    }
}
