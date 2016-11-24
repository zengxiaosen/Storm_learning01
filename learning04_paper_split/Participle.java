import jeasy.analysis.MMAnalyzer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/11/3.
 */
public class Participle {
    public   String[] participle(String content){
        String splitToken = "|";
        String result = null;
        MMAnalyzer analyzer = new MMAnalyzer();
        try{
            result = analyzer.segment(content, splitToken);
        }catch (IOException e){
            e.printStackTrace();
        }

        String[] result_out = result.split(splitToken);
        return result_out;

    }
}
