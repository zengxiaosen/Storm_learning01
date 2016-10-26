package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import sun.org.mozilla.javascript.internal.BaseFunction;
import trident.functions.MySplit01;
import trident.functions.Split;


import java.util.Random;

/**
 * Created by Administrator on 2016/10/26.
 */
public class TridentPVTopo {

    public static StormTopology buildTopology(LocalDRPC drpc){

        Random random = new Random();
        String[] hosts = {"www.taobao.com"};
        String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
                "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
        String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };
        StringBuffer sbBuffer = new StringBuffer();



        //这个3表示数据源里面3行为一个批次
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachLog"), 3,
                new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
                new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
                new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
                new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
                new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
                new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]));
        //改成false则数据只发送一次，如果是true则数据发送多次
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
                .parallelismHint(16)//并发度
                .each(new Fields("eachLog"), new MySplit01("\t"), new Fields("date", "session_id"))
                //根据session_id进行分组，分组之后搞一个pv
                .groupBy(new Fields("date")).persistentAggregate(new MemoryMapState.Factory(), new Fields("session_id"),new Count(), new Fields("PV"))
                .parallelismHint(16);
        //进行访问
        topology.newDRPCStream("GetPV", drpc).each(new Fields("args"), new Split(" "), new Fields("date")).groupBy(new Fields("date"))
                .stateQuery(wordCounts, new Fields("date"), new MapGet(), new Fields("PV")).each(new Fields("PV"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }
    public static void main(String[] args) throws Exception{
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if(args.length == 0){
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for(int i=0; i<100; i++){
                System.err.println("DRPC RESULT: " + drpc.execute("GetPV", "2014-01-07 2014-01-08"));
                Thread.sleep(1000);
            }
        }else{
            conf.setNumAckers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }
}
