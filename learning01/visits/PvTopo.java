package visits;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import base.SourceSpout;
import myfirst.MySpout;

import java.util.HashMap;
import java.util.Map;

public class PvTopo {

    /**
     * @param args
     * 多并发下是无法做全局汇总的
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new SourceSpout(), 1);
        builder.setBolt("bolt", new PVBolt(), 4).shuffleGrouping("spout");
        /*
        对单线程来讲，什么grouping都是一样的。。
         */
        builder.setBolt("sumBolt", new PVSumBolt(), 1).shuffleGrouping("bolt");

        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS, 4);

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }





    }

}
