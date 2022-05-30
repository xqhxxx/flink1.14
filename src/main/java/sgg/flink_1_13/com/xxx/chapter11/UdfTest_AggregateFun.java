package sgg.flink_1_13.com.xxx.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author xqh
 * @date 2022/5/30
 * @apiNote
 */
public class UdfTest_AggregateFun {
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
        tableEnv.createTemporarySystemFunction("MyFun", MyFun.class);

//        3调用  侧向表连接  lateral
        Table table = tableEnv.sqlQuery("select user,MyFun(ts,1) as w_avg from clickTable group by user ");

//        4打印
        tableEnv.toChangelogStream(table).print();

        env.execute();
    }

    //累加器类型
    public static class WeighterAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    //udf 聚合函数      计算加权平均
    public static class MyFun extends AggregateFunction<Long, WeighterAvgAccumulator> {

        @Override
        public Long getValue(WeighterAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }

        }

        @Override
        public WeighterAvgAccumulator createAccumulator() {
            return new WeighterAvgAccumulator();
        }

        //累加计算方法
        public void accumulate(WeighterAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }
    }

}
