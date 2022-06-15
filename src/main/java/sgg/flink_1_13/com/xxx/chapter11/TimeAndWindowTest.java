package sgg.flink_1_13.com.xxx.chapter11;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1 在创建表的DDl中直接定义时间属性
        String creatDDL = "create table clickTable(" +
                " user_name string," +
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

        // 2  在流转换成table的时候定义时间属性
//        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event event, long l) {
//                                return event.timestamp;
//                            }
//                        }));
//        Table clickTable = tableEnv.fromDataStream(ds, $("user"), $("url"), $("timestamp").as("ts"), $("et").rowtime());//$("et").proctime() 处理时间
//        clickTable.printSchema();

        //聚合查询转换
        // 1 分组
        Table aggTable = tableEnv.sqlQuery("select user_name,count(1) from clickTable  group by user_name");

        // 2 分组窗口聚合 1.12版本之前的
        Table gbTable = tableEnv.sqlQuery("select user_name,count(1) as cnt, TUMBLE_END(et,INTERVAL '10' SECOND) AS entT from  clickTable group by user_name,TUMBLE(et,INTERVAL '10' SECOND)");

        //        tableEnv.toChangelogStream(aggTable).print("agg；");
//        tableEnv.toChangelogStream(gbTable).print("gbt；");

        //todo 3  窗口聚合 tvf
        //3.1 滚动窗口
        Table tumbleTable = tableEnv.sqlQuery("select user_name,count(1) as cnt,window_end as endT from table( tumble(table clickTable,descriptor(et),interval '10' second)) group by user_name,window_end,window_start ");
//        tableEnv.toChangelogStream(tumbleTable).print("tumbleTable；");

        //3.2 滑动窗口  滑动间隔，窗口大小  ！！！注意 与dsAPI顺序相反
        Table hopTable = tableEnv.sqlQuery("select user_name,count(1) as cnt,window_end as endT from table(hop(table clickTable,descriptor(et),interval '5' second,interval '10' second)) group by user_name,window_end,window_start ");
//        tableEnv.toChangelogStream(hopTable).print("hopTable；");

        //3.3 累积窗口  间隔，窗口大小
        Table cumTable = tableEnv.sqlQuery("select user_name,count(1) as cnt,window_end as endT from table(cumulate(table clickTable,descriptor(et),interval '5' second,interval '10' second)) group by user_name,window_end,window_start ");
//        tableEnv.toChangelogStream(cumTable).print("cumTable；");


//        4 开窗聚合  over
        Table overTable = tableEnv.sqlQuery("select user_name,avg(ts)over( partition by user_name order by et rows between 3 preceding and current row) as avg_ts from clickTable");
        tableEnv.toChangelogStream(overTable).print("overTable；");

        env.execute();

    }
}
