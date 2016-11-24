package trident.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import sun.org.mozilla.javascript.internal.BaseFunction;
import tools.DataFmt;

import java.util.Map;

/**
 * Created by Administrator on 2016/10/26.
 */
public class MySplit01 extends BaseFunction implements Function {

    //序列化一下
    private static final long serialVersionUID = 1L;
    String patton = null;
    public MySplit01(String patton){
        this.patton = patton;
    }
    public void execute(TridentTuple tuple, TridentCollector collector) {
        //输入的这一个
        String log = tuple.getString(0);
        String logArr[] = log.split(patton);
        if(logArr.length == 3){
            //日期+session_id
            collector.emit(new Values(DataFmt.getCountDate(logArr[2], DataFmt.date_short),"cf","pv_count", logArr[1]));
        }

    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}