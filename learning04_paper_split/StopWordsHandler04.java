import java.io.File;

/**
 * Created by Administrator on 2016/11/4.
 */
public class StopWordsHandler04 {

    //停用词处理
    private static String stopWordList[] = {
            "的", "我们","要","自己","之","将","“","”","，","（","）","后","应","到","某","后","个","是","位","新","一","两","在","中","或","有","更","好",""
    };//常用停用词

    File file_stopword = new File("stop_words_ch.txt");



    public static boolean IsStopWord(String word){
        for(int i=0;i<stopWordList.length;++i){
            if(word.equalsIgnoreCase(stopWordList[i]))
                return true;
        }
        return false;
    }
}
