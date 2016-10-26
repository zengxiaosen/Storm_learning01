package com.zengxiaosen.adalgo.etl.hadoop_test;

import org.apache.hadoop.hive.ql.exec.UDF;

import javax.swing.text.Document;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/22.
 */
public class DocToMap extends UDF {

    /*
    publicMap<String,String>evaluate(Strings){


        returnDoc.deserialize(s);


    }
     */

    /*
    使用时的方法
    create external table if not exits test_table
    (
    doc string
    )
    stored as
    inputformat '..../DocFileInputFormat'
    outputformat '..../DocFileOutputFormat'
    location '/user/heyuan.1hy/doc/'

    add jar /xxxxx/hive-test.jar

    create temporary function doc_to_map as '...DocToMap';
    select
    raw['id'],
    raw['name']
    from
    (
    select
        doc_to_map(doc) raw
    from
        test_table
     ) t;
     */
}
