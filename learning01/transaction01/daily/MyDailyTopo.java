package transaction01.daily;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import transaction01.MyCommitter;
import transaction01.MyTransactionBolt;
import transaction01.MyTxSpout;

/**
 * Created by Administrator on 2016/10/25.
 */
public class MyDailyTopo {


    public static void main(String[] args){
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbId","spoutid",new MyTxSpout(),1);
        builder.setBolt("bolt1",new MyDailyBatchBolt(), 3).shuffleGrouping("spoutid");
        builder.setBolt("committer",new MyDailyCommitterBolt(), 1).shuffleGrouping("bolt1");

        Config conf = new Config();
        conf.setDebug(true);

        if (args.length > 0){
            try{
                StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
            }catch(AlreadyAliveException e){
                e.printStackTrace();
            }catch (InvalidTopologyException e){
                e.printStackTrace();
            }
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.buildTopology());
        }


    }
}
