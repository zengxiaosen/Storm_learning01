import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

/**
 * Created by Administrator on 2016/11/3.
 */
public class BayesClassifier {
    private TrainingDataManager tdm;//训练集管理器
    private String trainingDataPath;//训练集路径
    private static double zoomFactor = 10.0f;
    /*
    默认构造器，初始化训练集
     */
    public BayesClassifier(){
        tdm = new TrainingDataManager();
    }
    /*
    计算给定的文本属性向量x在给定的分类cj中的类条件概率
    参数 x 给定的文本属性向量
    参数 cj 给定的类别
    return 分类条件概率连乘值
     */
    float calcProd(String[] X, String Cj)
    {
        float ret = 1.0F;
        //类条件概率连乘
        for(int i=0; i< X.length; i++){
            String Xi = X[i];
            //因为结果过小，因此在连乘之前放大10倍，这对最终结果并无影响，因为我们只是比较概率大小而已
            ret *= ClassConditionalProbability.calculatePxc(Xi, Cj)*zoomFactor;//条件概率连乘
        }
        //再乘以先验概率
        ret *= PriorProbability.calculatePc(Cj);
        return ret;
    }
    /*
    去掉停用词
    参数 给定的文本
    return 去停用词后结果
     */
    public String[] DropStopWords(String[] oldWords){
        Vector<String> v1 = new Vector<String>();
        for(int i=0; i<oldWords.length; i++){
            if(StopWordsHandler.IsStopWord(oldWords[i]) == false){
                //不是停用词
                v1.add(oldWords[i]);
            }
        }
        String[] newWords = new String[v1.size()];
        v1.toArray(newWords);
        return newWords;
    }
    /*
    对给定的文本进行分类
    参数 text 给定的文本
    return 分类结果
     */
    @SuppressWarnings("unchecked")
    public String classify(String text){
        String[] terms = null;
        terms = ChineseSpliter.split(text, " ").split(" ");//中文分词处理，分词后结果可能还包含有停用词
        terms = DropStopWords(terms);//去掉停用词，以免影响分类
        String[] Classes = tdm.getTraningClassifications();//分类
        float probility = 0.0F;
        List<ClassifyResult> crs = new ArrayList<ClassifyResult>();//分类结果
        for(int i=0; i<Classes.length; i++){
            String Ci = Classes[i];//第i个分类
            probility = calcProd(terms, Ci);//计算给定的文本属性向量terms在给定的分类Ci中的分类条件概率
            //保存分类结果
            ClassifyResult cr = new ClassifyResult();
            cr.classification = Ci;//分类
            cr.probility = probility;//关键字在分类的条件概率
            System.out.println("In process...");
            System.out.println(Ci + ":" + probility);
            crs.add(cr);
        }
        //对最后概率结果进行排序
        java.util.Collections.sort(crs, new Comparator(){

            @Override
            public int compare(Object o1, Object o2) {
                final ClassifyResult m1 = (ClassifyResult) o1;
                final ClassifyResult m2 = (ClassifyResult) o2;
                final double ret = m1.probility - m2.probility;
                if(ret < 0){
                    return 1;
                }else{
                    return -1;
                }
            }
        });
        //返回概率最大的分类
        return crs.get(0).classification;

    }


    public static void main(String[] args){
        String text = "北京时间11月3日，据美媒体报道，今年NBA总决赛，勇士队在3-1领先的大好形势下被骑士队逆转夺冠。快船队小前锋保罗-皮尔斯在近日接受采访时认为与其说勇士队被骑士队击败，还不如说勇士队自己“休克”了。\n" +
                "\n" +
                "　　近日，皮尔斯做客名嘴比尔-西蒙斯的节目。采访中，西蒙斯问到了今年的NBA总决赛：“在你看来，到底是骑士队赢了？还是勇士队休克了？”\n" +
                "\n" +
                "　　皮尔斯考虑了一会儿，回答道：“勇士队休克了。”\n" +
                "\n" +
                "　　他随后解释道：“就好比说，真的，我的意思是3-1领先？你不应该输掉，3-1领先的情况下，你永远都不应该输掉系列赛。金州勇士自己休克了。”\n" +
                "\n" +
                "　　当被问到自己带领的球队是否会遭遇3-1领先被逆转时，皮尔斯坚定地表示：“这永远不可能发生。”\n" +
                "\n" +
                "　　事实上，这已经不是皮尔斯最近第一次抨击勇士队。之前，皮尔斯质疑了凯文-杜兰特加盟勇士队的决定，并认为这一代球员的竞争性不如上一代。为此，勇士队大前锋格林曾当面站出来为杜兰特辩护并回击皮尔斯。";
        BayesClassifier classifier = new BayesClassifier();//构造Bayes分类器
        String result = classifier.classify(text);//进行分类
        System.out.println("此项属于["+result+"]");
    }




}
