import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by Administrator on 2016/10/22.
 */
public class HiveJdbcClient {
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive://192.168.153.100:10000/default";
    private static String user = "hive";
    private static String password = "hive";
    private static String sql = "";
    private static ResultSet res;
    private static final Logger log = Logger.getLogger(HiveJdbcClient.class);
    public static void main(String[] args){
        try{
            Class.forName(driverName);
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();
            //创建的表名
            String tableName = "testHiveJDBC";
            //第一步，存在就先删除
            sql = "drop table" + tableName;
            stmt.executeQuery(sql);
            //第二步，不存在就创建
            sql = "create table " + tableName + " (key int, value string) " +
                    "row format delimited fields terminated by '\t'";
        }catch (ClassNotFoundException e){
            e.printStackTrace();
            log.error(driverName+"not found!!!!!", e);
            System.exit(1);
        }catch (SQLException e){
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        }
    }


}
