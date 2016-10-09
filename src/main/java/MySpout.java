import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.metric.SystemBolt;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout implements IRichSpout{

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;

    SpoutOutputCollector collector = null;


    String str = null;


    public void nextTuple() {
        try {
            while ((str = this.br.readLine()) != null) {
                // 过滤动作

                collector.emit(new Values(str));

//				Thread.sleep(3000);
                //to do
            }
        } catch (Exception e) {
            // TODO: handle exception
        }


    }


    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        try {
            this.collector = collector;
            this.fis = new FileInputStream("track.log");
            this.isr = new InputStreamReader(fis, "UTF-8");
            this.br = new BufferedReader(isr);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // TODO Auto-generated method stub

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("log"));
    }


    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    public void ack(Object msgId) {
        // TODO Auto-generated method stub
        System.out.println("spout ack:"+msgId.toString());

    }


    public void activate() {
        // TODO Auto-generated method stub

    }


    public void close() {
        // TODO Auto-generated method stub
        try{
            br.close();
            isr.close();
            fis.close();

        }catch (Exception e){
            e.printStackTrace();
        }

    }


    public void deactivate() {
        // TODO Auto-generated method stub

    }


    public void fail(Object msgId) {
        // TODO Auto-generated method stub
        System.out.println("spout fail:"+msgId.toString());

    }

}
