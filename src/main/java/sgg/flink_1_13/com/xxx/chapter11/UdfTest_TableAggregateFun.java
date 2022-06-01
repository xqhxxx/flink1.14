package sgg.flink_1_13.com.xxx.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author xqh
 * @date 2022/5/30
 * @apiNote
 */
public class UdfTest_TableAggregateFun {
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

//      2 注册 表聚合函数
//        tableEnv.registerFunction("Top2", new Top2());//不推荐 用ctsf代替
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);


//        3调用  侧向表连接  lateral

        String wAggQuery = "select user,count(url) as cnt,window_start,window_end from table(tumble( table clickTable,descriptor(et),interval '10' second )) group by user,window_start,window_end";

        Table aggTable = tableEnv.sqlQuery(wAggQuery);
//        tableEnv.toChangelogStream(aggTable).print();
        //对sql的表聚合函数 目前不支持
//     1.14 15   在 Table API 查询中使用函数（当前只在 Table API 中支持 TableAggregateFunction）
        Table restable = aggTable.groupBy($("window_end"))
                .flatAggregate(call("Top2", $("cnt")).as("cnt", "rank"))
                .select($("window_end"), $("cnt"), $("rank"));

//        4打印
        tableEnv.toChangelogStream(restable).print();

        env.execute();
    }

    //累加器类型
    public static class TopNAccumulator {
        public Long first;
        public Long second;
    }

    //udf 表聚合函数     top2
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, TopNAccumulator> {
        @Override
        public TopNAccumulator createAccumulator() {
            TopNAccumulator topNAccumulator = new TopNAccumulator();
            topNAccumulator.first = Long.MIN_VALUE;
            topNAccumulator.second = Long.MIN_VALUE;
            return topNAccumulator;
        }

        //        更新累加器
        public void accumulate(TopNAccumulator acc, Long value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        //得自己写 没有override方法
        public void emitValue(TopNAccumulator acc, Collector<Tuple2<Long, Integer>> out) {
            if (acc.first != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }

        }


    }

}
