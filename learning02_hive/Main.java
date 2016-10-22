import java.io.File;

/**
 * Created by Administrator on 2016/10/12.
 */
public class Main {

    public static void main(String[] args) throws Exception{
        if(args == null || args.length == 0){
            System.out.println("请输入参数，第一个参数为sql文件的全路径，第二，三等为sql语句中的参数，：-key value,例如" +
                    "hiveF /opt/bin/jar/test.sql -yesterday 20151015 -today 20151016 -hour 15");
            return;
        }
        ParseArgs parse = new ParseArgs(args);
        String sql = Utils.getSql(new File(args[0]));
        System.out.println(Utils.parse(sql, parse.getMap()));
    }
}

/*
shell 脚本编写
#!/bin/bash
source /etc/profile
sql='/usr/java/jdk1.7.0_67-cloudera/bin/java -jar /opt/bin/hiveF/jars/hiveF.jar $*'
echo "$sql"

#hive -e "$sql"
if [["$sql" =~ ^请输入参数 ]]; then
exit
else
hive -e "$sql"
fi

 */