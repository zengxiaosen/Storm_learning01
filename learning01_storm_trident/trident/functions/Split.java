package trident.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import sun.org.mozilla.javascript.internal.BaseFunction;

import java.util.Map;

/**
 * Created by Administrator on 2016/10/26.
 */
public  class Split extends BaseFunction implements Function {
    private static final long serialVersionUID = 1L;
    String patton = null;
    public Split(String patton){
        this.patton = patton;
    }
    public void execute(TridentTuple tuple, TridentCollector collector){
        String sentence = tuple.getString(0);
        for(String word : sentence.split(patton)){
            collector.emit(new Values(word));
        }
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
