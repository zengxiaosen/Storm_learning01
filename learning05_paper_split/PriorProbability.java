/**
 * Created by Administrator on 2016/11/3.
 */
public class PriorProbability {
    //先验概率计算
    private static TrainingDataManager tdm = new TrainingDataManager();
    /*
    参数：给定的分类
    return 给定条件下的先验概率
     */
    public static float calculatePc(String c){
        float ret = 0F;
        float Nc = tdm.getTrainingFileCountOfClassification(c);
        float N = tdm.getTrainingFileCount();
        ret = Nc / N;
        return ret;
    }
}
