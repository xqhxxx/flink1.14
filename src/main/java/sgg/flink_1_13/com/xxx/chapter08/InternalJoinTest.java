package sgg.flink_1_13.com.xxx.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/5/6
 * @apiNote
 */
public class InternalJoinTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> ds1 = env.fromElements(
                Tuple2.of("aaa", 3000L),
                Tuple2.of("ddd", 4000L),
                Tuple2.of("ddd", 4500L),
                Tuple2.of("aaa", 5500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));


        SingleOutputStreamOperator<Event> ds2 = env.fromElements(
                new Event("aaa", "/jkl", 2000l),
                new Event("ddd", "/jkl", 20000L),
                new Event("ddd", "/jkl", 10000L),
                new Event("aaa", "/jkl", 1000L),
                new Event("aaa", "/jkl", 5500L)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        ds1.keyBy(x -> x.f0)
                .intervalJoin(ds2.keyBy(x -> x.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> l, Event r, ProcessJoinFunction<Tuple2<String, Long>, Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(r + " => " + l);
                    }
                }).print();


        env.execute();

    }
}
