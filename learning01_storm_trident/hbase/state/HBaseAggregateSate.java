package hbase.state;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.IBackingMap;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
@SuppressWarnings({"rawtypes","unchecked"})
/**
 * Created by Administrator on 2016/11/3.
 */
public class HBaseAggregateSate<T> implements IBackingMap<T> {

    private HTableConnector connector;
    private Serializer<T> serializer;
    public HBaseAggregateSate(TridentConfig config){
        this.serializer = config.getStateSerializer();
        try {
            this.connector = new HTableConnector(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static StateFactory opaque(TridentConfig<OpaqueValue> config){

        return new HBaseAggregateFactory(config, StateType.OPAQUE);
    }
    public static StateFactory transactional(TridentConfig<OpaqueValue> config){
        return new HBaseAggregateFactory(config, StateType.TRANSACTIONAL);
    }
    public static StateFactory nonTransactional(TridentConfig<OpaqueValue> config){
        return new HBaseAggregateFactory(config, StateType.NON_TRANSACTIONAL);
    }



    @Override
    public List<T> multiGet(List<List<Object>> list) {
        //批量从数据库里面查
        List<Get> gets = new ArrayList<Get>(list.size());
        byte[] rk;
        byte[] cf;
        byte[] cq;
        for(List<Object> k : list){
            rk = Bytes.toBytes((String)k.get(0));
            cf = Bytes.toBytes((String)k.get(1));
            cq = Bytes.toBytes((String)k.get(2));

            Get get = new Get(rk);
            gets.add(get.addColumn(cf, cq));

        }
        Result[] results = null;
        try {
            results = connector.getTable().get(gets);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //查出来做一个反序列化，然后把list当做结果返回出来
        List<T> rtn = new ArrayList<T>(list.size());
        for(int i=0; i< list.size(); i++){
            cf = Bytes.toBytes((String)list.get(i).get(1));
            cq = Bytes.toBytes((String)list.get(i).get(2));
            Result result = results[i];
            if(result.isEmpty()){
                rtn.add(null);
            }else{
                rtn.add((T)serializer.deserialize(result.getValue(cf, cq)));
            }

        }
        return rtn;
    }

    @Override
    public void multiPut(List<List<Object>> k, List<T> vals) {

        //从参数里面得到值，put到hbase里面
        List<Put> puts = new ArrayList<Put>();

        for(int i=0; i<k.size(); i++){
            byte[] rk = Bytes.toBytes((String)k.get(i).get(0));
            byte[] cf = Bytes.toBytes((String)k.get(i).get(1));
            byte[] cq = Bytes.toBytes((String)k.get(i).get(2));
            //vals里边的数写进去必须进行序列化
            byte[] cv = serializer.serialize(vals.get(i));
            Put p = new Put(rk);
            puts.add(p.add(cf,cq,cv));
        }
        try{
            connector.getTable().put(puts);
            connector.getTable().flushCommits();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
