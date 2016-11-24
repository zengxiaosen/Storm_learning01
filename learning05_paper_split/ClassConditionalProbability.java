/**
 * Created by Administrator on 2016/11/3.
 */
public class ClassConditionalProbability {
    //条件概率
    //这是另一个影响因子，和先验概率一起决定最终结果
    private static TrainingDataManager tdm = new TrainingDataManager();
    private static final float M = 0F;

    public static float calculatePxc(String x, String c){
        float ret = 0F;
        float Nxc = tdm.getCountContainKeyOfClassification(c, x);
        float Nc = tdm.getTrainingFileCountOfClassification(c);
        float V = tdm.getTraningClassifications().length;
        ret = (Nxc + 1) / (Nc + M + V); //为了避免出现0这样极端情况，进行加权处理
        return ret;
    }
}
