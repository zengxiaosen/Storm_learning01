package hbase.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2016/10/27.
 */
public class TupleTableConfig implements Serializable{
    //基础类，实现hbase表的一些定义之类的
    private String tableName;
    protected String tupleRowKeyField;
    protected String tupleTimestampField;
    protected Map<String, Set<String>> columnFamily;
    public TupleTableConfig(final String table, final String rowkeyField){
        this.tableName = table;
        this.tupleRowKeyField = rowkeyField;
        this.tupleTimestampField = "";
        this.columnFamily = new HashMap<String, Set<String>>();
    }

    public TupleTableConfig(final String table, final String rowkeyField, final String tupleTimestampField){
        this.tableName = table;
        this.tupleRowKeyField = rowkeyField;
        this.tupleTimestampField = tupleTimestampField;
        this.columnFamily = new HashMap<String, Set<String>>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTupleRowKeyField() {
        return tupleRowKeyField;
    }

    public void setTupleRowKeyField(String tupleRowKeyField) {
        this.tupleRowKeyField = tupleRowKeyField;
    }

    public String getTupleTimestampField() {
        return tupleTimestampField;
    }

    public void setTupleTimestampField(String tupleTimestampField) {
        this.tupleTimestampField = tupleTimestampField;
    }

    public Map<String, Set<String>> getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(Map<String, Set<String>> columnFamily) {
        this.columnFamily = columnFamily;
    }


}
