import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by Administrator on 2016/10/23.
 */
public class YYTingToken extends UDF {
    public Long evaluate(String s){
        try {
            if(s == null){
                return null;
            }else{
                //随便写
            }
        }catch (Exception e){
            return null;
        }
        return null;
    }
}
