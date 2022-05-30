package sgg.flink_1_13.com.xxx.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author xqh
 * @date 2022/5/30
 * @apiNote
 */
public class UdfTest_TableFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1 在创建表的DDl中直接定义时间属性
        String creatDDL = "create table clickTable(" +
                " `user` string," +
                " url string," +
                " ts BIGINT, " +
                " et as TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) ), " +
                " WATERMARK FOR et AS et- INTERVAL '1' SECOND " +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/click.csv', " +
                " 'format' = 'csv'" +
                ") ";

        tableEnv.executeSql(creatDDL);

//      2 注册ud表函数
        tableEnv.createTemporarySystemFunction("MyFun",MyFun.class);

//        3调用  侧向表连接  lateral
        Table table = tableEnv.sqlQuery("select user,url,word,length from clickTable,lateral table(MyFun(url)) as T(word,length)  ");

//        4打印
        tableEnv.toChangelogStream(table).print();

        env.execute();
    }

    //udf 表函数  1-》n
    public static class MyFun extends TableFunction<Tuple2<String,Integer>> {
        public void eval(String str) {
            String[] split = str.split("\\?");
            for (String s : split) {
                collect(Tuple2.of(s,s.length()));
            }
        }
    }

}
