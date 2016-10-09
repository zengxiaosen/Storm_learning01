package user_visit;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import tools.DataFmt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/6.
 */
public class UVSumBolt implements IBasicBolt{



    private static final long serialVersionUID = 1L;
    String cur_date = null;
    long beginTime = System.currentTimeMillis();
    long endTime = 0;

    Map<String, Integer> counts = new HashMap<String, Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        cur_date = DataFmt.getCountDate("2014-01-07", DataFmt.date_short);
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        try{
            endTime = System.currentTimeMillis();
            long PV = 0;//总数
            long UV = 0;//个数，去重后
            String dateSession_id = tuple.getString(0);
            Integer countInteger = tuple.getInteger(1);

            //判断数据是不是当天的,而且比当前日期还要打
            if(!dateSession_id.startsWith(cur_date) && DataFmt.parseDate(dateSession_id.split("_")[0]).after(DataFmt.parseDate(cur_date))){
                cur_date = dateSession_id.split("_")[0];
                counts.clear();
            }


            counts.put(dateSession_id, countInteger);


            if(endTime - beginTime >= 2000){

                //获取word去重个数，遍历counts的keyset，取count
                Iterator<String> i2 = counts.keySet().iterator();
                while(i2.hasNext()){
                    String key = i2.next();
                    if(key != null){
                        if(key.startsWith(cur_date)){
                            UV ++;
                            PV += counts.get(key);
                        }
                    }
                }
                System.out.println("PV=" + PV + "; UV=" + UV);


            }





        }catch (Exception e){
            throw new FailedException("SumBolt fail!");
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
