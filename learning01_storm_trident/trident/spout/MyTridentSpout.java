package trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import transaction01.MyData;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Administrator on 2016/10/26.
 */
public class MyTridentSpout implements ITridentSpout<MyData>{
    private static final long serialVersionUID = 1L;
    @Override
    public BatchCoordinator<MyData> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return new MyBatchCoordinator();
    }

    @Override
    public Emitter<MyData> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return new MyEmitter();
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }

    public class MyBatchCoordinator implements BatchCoordinator<MyData>{

        @Override
        public MyData initializeTransaction(long l, MyData myData, MyData x1) {
            return null;
        }

        @Override
        public void success(long l) {

        }

        @Override
        public boolean isReady(long l) {
            return true;
        }

        @Override
        public void close() {

        }
    }
    public class MyEmitter implements Emitter<MyData>{

        @Override
        public void emitBatch(TransactionAttempt transactionAttempt, MyData myData, TridentCollector tridentCollector) {

            Random random = new Random();
            String[] hosts = {"www.taobao.com"};
            String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
                    "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
            String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53",
                    "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };
            StringBuffer sbBuffer = new StringBuffer();
            for (long i = 0; i < 100; i++){
                tridentCollector.emit(new Values(i, hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]+"\n"));

            }
        }

        @Override
        public void success(TransactionAttempt transactionAttempt) {

        }

        @Override
        public void close() {

        }
    }
}
