package transaction01;

import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

import java.math.BigInteger;

/**
 * Created by Administrator on 2016/10/24.
 */
public class MyCoordinator implements ITransactionalSpout.Coordinator<MyData> {
    public static int BATCH_NUM = 10;
    @Override
    public MyData initializeTransaction(BigInteger txid, MyData preMetadata) {
        long beginPoint = 0;
        if(preMetadata == null){
            beginPoint = 0;
        }else{
            beginPoint = preMetadata.getBeginPoint() + preMetadata.getNum();
        }
        MyData mata = new MyData();
        mata.setBeginPoint(beginPoint);
        mata.setNum(BATCH_NUM);

        //这个方法就是返回元数据，这个方法里面包含了事务的开始位置以及事务的数量
        System.err.println("启动一个事务："+mata.toString());
        return mata;
    }

    @Override
    public boolean isReady() {
        //没启动一个事务的时候，我们停留两秒
        Utils.sleep(2000);
        return true;
    }

    @Override
    public void close() {

    }
}
