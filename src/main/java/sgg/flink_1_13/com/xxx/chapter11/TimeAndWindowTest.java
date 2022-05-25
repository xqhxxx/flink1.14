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
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1 在创建表的DDl中直接定义时间属性
        String creatDDL = "create table clickTable(" +
                " user_name string," +
                " url string," +
                " ts BIGINT, " +
                " et as TO_TIMESTAMP( FROM_UNIXTIME(TS/1000) ), "+
                " WATERMARK FOR et AS et- INTERVAL '1' SECOND "+
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/click.csv', " +
                " 'format' = 'csv'" +
                ") ";

        // 2  在流转换成table的时候定义时间属性
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        Table table = tableEnv.fromDataStream(ds, $("user"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());//$("et").proctime() 处理时间
        table.printSchema();

    }
}
