package transaction01;

import java.io.Serializable;

/**
 * Created by Administrator on 2016/10/24.
 */
public class MyData implements Serializable {

    public long getBeginPoint() {
        return beginPoint;
    }

    public void setBeginPoint(long beginPoint) {
        this.beginPoint = beginPoint;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "MyData{" +
                "beginPoint=" + beginPoint +
                ", num=" + num +
                '}';
    }

    private long beginPoint;//事务开始位置
    private int num;//batch 的tuple 个数

}
