package sgg.flink_1_13.com.xxx.chapter11;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author xqh
 * @date 2022/5/16
 * @apiNote
 */
public class SimpleTableExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

//        ds.print("ds:");

        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3 ds转换为table
        Table table1 = tableEnv.fromDataStream(ds);
//        table1.printSchema();

        //4 写sql
        Table rt = tableEnv.sqlQuery("select user,url,`timestamp` from " + table1);
//        tableEnv.toDataStream(rt).print("result:");

        //5 基于table
        Table rs2 = table1.select($("user"), $("url"))
                .where($("user").isEqual("a"));

        //6 表转换为流 打印输出
//        tableEnv.toDataStream(rs2).print("rs2:");


        //7 聚合
        tableEnv.createTemporaryView("clickTable",table1);
        Table agg = tableEnv.sqlQuery("select user ,count(url) from clickTable group by user");

//        tableEnv.toDataStream(agg).print("agg:");  group by  不再是简单的追加打印
        tableEnv.toChangelogStream(agg).print("agg:");//而是更新操作
        //建议直接调用 toChangelogStream


        env.execute();
    }
}
