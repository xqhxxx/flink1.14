package sgg.flink_1_13.com.xxx.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author xqh
 * @date 2022/5/30
 * @apiNote
 */
public class UdfTest_ScalaFun {

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

//      2 注册udf
        tableEnv.createTemporarySystemFunction("MyFun",MyFun.class);

//        3调用
        Table table = tableEnv.sqlQuery("select user,MyFun(user) from clickTable");

//        4打印
        tableEnv.toChangelogStream(table).print();

        env.execute();

    }

    //udf 标量函数  1-》1
    public static class MyFun extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }

    }

}
