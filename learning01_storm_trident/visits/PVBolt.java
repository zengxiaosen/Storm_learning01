package visits;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.ZookeeperAuthInfo;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.zookeeper.*;

import java.net.InetAddress;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/6.
 * storm kill作业
 * storm kill PvTopo
 * storm提交作业
 * storm jar ./starter.jar visits.PvTopo PvTopo
 *
 */
public class PVBolt implements IRichBolt {


    /*
    这种irichbolt形式就是成功的时候要显性的调ack方法
    失败的时候掉fail方法
     */
    private static final long serialVersionUID = 1L;

    /*
    执行前需要在zookeeper上把目录建立一下
    zkCli.sh -server localhost:2181
    ls /
    create /lock ""
    create /lock/storm ""
    ls /lock
    这样就创建好了
     */
    public static final String zk_path = "/lock/storm/pv";
    ZooKeeper zKeeper = null;
    String lockData = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            zKeeper = new ZooKeeper("192.168.1.107:2181,192.168.1.108:2181",3000,new Watcher(){

                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("event:"+watchedEvent.getType());
                }
            });
            while (zKeeper.getState() != ZooKeeper.States.CONNECTED){
                Thread.sleep(1000);
            }

            InetAddress address = InetAddress.getLocalHost();
            //ip地址和taskip的组合肯定是唯一的
            lockData = address.getHostAddress() + ":" + topologyContext.getThisTaskId();
            //false的意思是不放监听上去
            if(zKeeper.exists(zk_path, false) == null){
                zKeeper.create(zk_path, lockData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }

        }catch(Exception e){
            try {
                zKeeper.close();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

        }
    }

    String logString = null;
    String sessionid = null;
    //static long Pv = 0;
    long Pv = 0;
    long beginTime = System.currentTimeMillis();
    long endTimes = 0;
    @Override
    public void execute(Tuple tuple) {
        try{
            endTimes =  System.currentTimeMillis();
            logString = tuple.getString(0);
            if (logString != null){
                sessionid = logString.split("\t")[1];

       /* *//*
        这种多线程下做计算我们还必须得有synchronized使它线程安全
        这样肯定就和单线程一样
        然而还是不够健全，因为synchronized和lock在单jvm下有效，单在多jvm下无效

         *//*
        synchronized (this){
            if(sessionid != null){
                Pv ++;
            }
        }*/

                //shuffleGrouping下，pv* Executor并发数就是统计的pv
                //因为shufflegrouping是平均分配，而我们有两个线程
                //
                if(sessionid != null){
                    Pv ++;
                }



            }

            if (endTimes - beginTime >= 5*1000){
                System.err.println(lockData+"=========================");
                if(lockData.equals(zKeeper.getData(zk_path, false, null))){
                    System.out.println("pv ================== "+ Pv * 4);
                }
                beginTime = System.currentTimeMillis();
            }


        }catch(Exception e){
            e.printStackTrace();
        }


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
