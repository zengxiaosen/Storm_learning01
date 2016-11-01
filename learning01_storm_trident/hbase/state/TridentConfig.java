package hbase.state;

import storm.trident.state.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/27.
 */
public class TridentConfig extends TupleTableConfig{

    private static final long serialVersionUID = 1l;

    public int getStateCacheSize() {
        return stateCacheSize;
    }

    public void setStateCacheSize(int stateCacheSize) {
        this.stateCacheSize = stateCacheSize;
    }

    public Serializer getStateSerializer() {
        return stateSerializer;
    }

    public void setStateSerializer(Serializer stateSerializer) {
        this.stateSerializer = stateSerializer;
    }

    private int stateCacheSize = 1000;
    private Serializer stateSerializer;

    public static final Map<StateType, Serializer> DEFALT_SERIALIZES = new HashMap<StateType, Serializer>(){
        //让state支持三种语义
        {
            put(StateType.NON_TRANSACTIONAL,new JSONNonTransactionalSerializer());
            put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
            put(StateType.OPAQUE, new JSONOpaqueSerializer());
        }
    };
    public TridentConfig(String table, String rowKeyField){
        super(table, rowKeyField);
    }
    public TridentConfig(String table, String rowkeyField, String tupleTimestampField){
        super(table, rowkeyField, tupleTimestampField);
    }

}
