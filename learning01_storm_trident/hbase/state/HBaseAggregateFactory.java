package hbase.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.*;

import java.util.Map;

/**
 * Created by Administrator on 2016/11/3.
 */
//它的作用就是泛型的地方把它忽略掉
@SuppressWarnings({"rawtypes","unchecked"})
public class HBaseAggregateFactory implements StateFactory{
    private static final long serialVersionUID = 1L;
    private StateType type;
    private TridentConfig config;

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        HBaseAggregateSate state = new HBaseAggregateSate(config);
        CachedMap cachedMap = new CachedMap(state, config.getStateCacheSize());

        MapState ms = null;
        if(type == StateType.NON_TRANSACTIONAL){
            ms = NonTransactionalMap.build(cachedMap);
        }else if (type == StateType.OPAQUE){
            ms = OpaqueMap.build(cachedMap);
        }else if (type == StateType.NON_TRANSACTIONAL){
            ms = TransactionalMap.build(cachedMap);
        }
        return new SnapshottableMap(ms, new Values("$GLOBAL$"));
    }

    public HBaseAggregateFactory(TridentConfig config, StateType type){
        this.config = config;
        this.type = type;

        if(config.getStateSerializer() == null){
            config.setStateSerializer(TridentConfig.DEFALT_SERIALIZES.get(type));
        }

    }
}
