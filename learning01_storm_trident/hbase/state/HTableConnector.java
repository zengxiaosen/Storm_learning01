package hbase.state;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Administrator on 2016/10/27.
 */
public class HTableConnector implements Serializable{
    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public HTable getTable() {
        return table;
    }

    public void setTable(HTable table) {
        this.table = table;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private Configuration configuration;
    protected HTable table;
    private String tableName;

    public HTableConnector(TupleTableConfig conf) throws Exception{
        this.tableName = conf.getTableName();
        this.configuration = HBaseConfiguration.create();
        String filePathString = "hbase-site.xml";
        Path path = new Path(filePathString);
        this.configuration.addResource(path);
        this.table = new HTable(this.configuration, this.tableName);

    }
    public void close(){
        try {
            this.table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
