package transaction01;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Administrator on 2016/10/24.
 */


public class MyTxSpout implements ITransactionalSpout <MyData> {
    /*
    数据源
     */


    Map<Long, String> dbMap = null;
    //这样数据源就是我们这里的一个map
    public MyTxSpout(){
        Random random = new Random();
        dbMap = new HashMap<Long, String>();
        String[] hosts = {"www.taobao.com"};
        String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
                "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
        String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };
        StringBuffer sbBuffer = new StringBuffer();
        for (long i = 0; i < 100; i++){
            dbMap.put(i, hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]+"\n");

        }
    }

    private static final long serialVersionUID = 1L;
    @Override
    public Coordinator<MyData> getCoordinator(Map map, TopologyContext topologyContext) {
        return new MyCoordinator();
    }

    @Override
    public Emitter<MyData> getEmitter(Map map, TopologyContext topologyContext) {
        return new MyEmitter(dbMap);
    }

    @Override
    //这个就是定义一下类型
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx","log"));

    }

    //这个就是定义下storm的配置文件，然后配置成map
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
