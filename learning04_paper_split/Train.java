import org.apache.avro.generic.GenericData;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2016/11/3.
 */
public class Train implements Serializable {
    private  static  final long serialVersionUID = 1L;
    public final static String SERIALIZABLE_PATH = "D:\\Train.ser";
    //训练集的位置
    private String trainPath = "D:\\Reduced";
    //类别序号对应的实际名称
    private Map<String, String> classMap = new HashMap<String, String>();
    //类别对应的txt文本数
    private Map<String, Integer> classP = new ConcurrentHashMap<String, Integer>();
    //所有文本数
    private AtomicInteger actCount = new AtomicInteger(0);
    //每个类别对应的词典和频数
    private Map<String, Map<String, Double>> classwordMap = new ConcurrentHashMap<String, Map<String, Double>>();
    //分词器
    private transient Participle participle;
    private static Train trainInstance = new Train();
    private static Train getInstance(){
        trainInstance = new Train();
        //读取序列化在硬盘的本类对象
        FileInputStream fis;
        try{
            File f = new File(SERIALIZABLE_PATH);
            if(f.length() != 0){
                fis = new FileInputStream(SERIALIZABLE_PATH);
                ObjectInputStream oos = new ObjectInputStream(fis);
                trainInstance = (Train) oos.readObject();
                trainInstance.participle = new IkParticiple();
            }else{
                trainInstance = new Train();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return trainInstance;
    }

    private Train(){
        this.participle = new IkParticiple();
    }
    public String readtxt(String path){
        BufferedReader br = null;
        StringBuilder str = null;
        try {
            br = new BufferedReader(new FileReader(path));
            str = new StringBuilder();
            String r = br.readLine();
            while(r != null){
                str.append(r);
                r = br.readLine();
            }
            return str.toString();
        }catch (IOException ex){
            ex.printStackTrace();
        }finally {
            if(br != null){
                try{
                    br.close();
                }catch (IOException e){
                    e.printStackTrace();
                }

            }
            str = null;
            br = null;
        }
        return "";
    }

    /*
    训练数据
     */
    public void realTrain(){
        //初始化
        classMap = new HashMap<String, String>();
        classP = new HashMap<String, Integer>();
        actCount.set(0);
        classwordMap = new HashMap<String, Map<String, Double>>();

        // classMap.put("C000007", "汽车");
        classMap.put("C000008", "财经");
        classMap.put("C000010", "IT");
        classMap                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           .put("C000013", "健康");
        classMap.put("C000014", "体育");
        classMap.put("C000016", "旅游");
        classMap.put("C000020", "教育");
        classMap.put("C000022", "招聘");
        classMap.put("C000023", "文化");
        classMap.put("C000024", "军事");

        //计算各个类别的样本数
        Set<String> keySet = classMap.keySet();
        //所有词汇的集合，是为了计算每个单词在多少篇文章中出现，用于后面计算df
        final Set<String> allWords = new HashSet<String>();
        //存放每个类别的文件词汇内容
        final Map<String, List<String[]>> classContentMap = new ConcurrentHashMap<String, List<String[]>>();

        for(String classKey : keySet){
            Participle participle = new IkParticiple();
            Map<String, Double> wordMap = new HashMap<String, Double>();
            File f = new File(trainPath + File.separator + classKey);
            File[] files = f.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    if(pathname.getName().endsWith(".txt")){
                        return true;
                    }
                    return false;
                }
            });

            //存储每个类别文件词汇向量
            List<String[]> fileContent = new ArrayList<String[]>();
            if(files != null){
                for(File txt : files){
                    String content = readtxt(txt.getAbsolutePath());
                    //分词
                    String[] word_arr = participle.participle(content);
                    fileContent.add(word_arr);
                    //统计每个词出现的个数
                    for(String word : word_arr){
                        if(wordMap.containsKey(word)){
                            Double wordCount = wordMap.get(word);
                            wordMap.put(word, wordCount + 1);
                        }else{
                            wordMap.put(word, 1.0);
                        }
                    }
                }
            }

            //每个类别对应的词典和频数
            classwordMap.put(classKey, wordMap);
            //每个类别的文章数目
            classP.put(classKey, files.length);
            //所有文本数
            actCount.addAndGet(files.length);
            classContentMap.put(classKey, fileContent);

        }
        //把训练好的训练器对象序列化到本地（空间换时间）
        FileOutputStream fos;
        try{
            fos = new FileOutputStream(SERIALIZABLE_PATH);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /*
    分类
    返回各个类别的概率大小
     */
    @SuppressWarnings("unchecked")
    public String classify(String text){
        //分词，并去重
        String[] text_words = participle.participle(text);
        text_words = DropStopWords(text_words);//去掉停用词，以免影响分类
        Set<String> keySet = classMap.keySet();
        List<ClassifyResult04> crs = new ArrayList<ClassifyResult04>();//分类结果
        for(String classKey : keySet){
            double typeOfThis = 1.0;
            Map<String, Double> wordMap = classwordMap.get(classKey);
            for(String word : text_words){
                Double wordCount = wordMap.get(word);
                int articleCount = classP.get(classKey);

                /*
                Double wordidf = idfMap.get(word);
                if(wordidf == null){
                wordidf = 0.001;
                }else{
                wordidf = Math.log(actCount / wordidf);
                }
                 */

                //假如这个词在类别下所有文章中木有，那么给定个极小的值， 不影响计算
                double term_frequency = (wordCount == null) ? ((double) 1 / (articleCount + 1)) : (wordCount / articleCount);
                //文本在类别的概率，在这里按照特征向量独立统计，即概率=词汇1/文章数 * 词汇2/文章数 ...
                //当double无限小的时候会归为0，为了避免0 *10
                typeOfThis = typeOfThis * term_frequency * 10;
                typeOfThis = ((typeOfThis == 0.0) ? Double.MIN_VALUE : typeOfThis);


            }
            typeOfThis = ((typeOfThis == 1.0) ? 0.0 : typeOfThis);

            //此类别文章出现的概率
            double classOfAll = classP.get(classKey) / actCount.doubleValue();
            //根据贝叶斯公式 $(A|B) = S(B|A)*S(A)/S(B),由于$(B)是常数，在这里不做计算，不影响分类结果



            ClassifyResult04 cr = new ClassifyResult04();
            cr.classification = classKey;//分类
            cr.probility = typeOfThis * classOfAll;//关键字在分类的条件概率
            System.out.println("In process...");
            System.out.println(classKey + ":" + cr.probility);
            crs.add(cr);


        }

        //对最后概率结果进行排序
        java.util.Collections.sort(crs, new Comparator(){

            @Override
            public int compare(Object o1, Object o2) {
                final ClassifyResult04 m1 = (ClassifyResult04) o1;
                final ClassifyResult04 m2 = (ClassifyResult04) o2;
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

    public void printAll(){
        Set<Map.Entry<String, Map<String, Double>>> classWordEntry = classwordMap.entrySet();
        for(Map.Entry<String, Map<String, Double>> ent : classWordEntry){
            System.out.println("类别： " + ent.getKey());
            Map<String, Double> wordMap = ent.getValue();
            Set<Map.Entry<String, Double>> wordMapSet = wordMap.entrySet();
            for(Map.Entry<String, Double> wordEnt : wordMapSet){
                System.out.println(wordEnt.getKey() + ":" + wordEnt.getValue());
            }
        }

    }

    public Map<String, String> getClassMap(){
        return classMap;
    }

    public void setClassMap(Map<String, String> classMap){
        this.classMap = classMap;
    }

    public static void main(String[] args){
        Train mytrain = Train.getInstance();
        mytrain.realTrain();
        //mytrain.printAll();

        String content_test = "北京时间11月3日，据美媒体报道，今年NBA总决赛，勇士队在3-1领先的大好形势下被骑士队逆转夺冠。快船队小前锋保罗-皮尔斯在近日接受采访时认为与其说勇士队被骑士队击败，还不如说勇士队自己“休克”了。\n" +
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

        String static_test = mytrain.classify(content_test);
        System.out.println("此项属于["+static_test+"]");

    }



}
