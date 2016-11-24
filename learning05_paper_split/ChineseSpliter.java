import jeasy.analysis.MMAnalyzer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/11/3.
 */
public class ChineseSpliter {
    //中文分词
    public static String split(String text, String splitToken){
        String result = null;
        MMAnalyzer analyzer = new MMAnalyzer();
        try{
            result = analyzer.segment(text, splitToken);
        }catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }
}
