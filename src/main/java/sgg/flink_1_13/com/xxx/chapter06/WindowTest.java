package sgg.flink_1_13.com.xxx.chapter06;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/4/8
 * @apiNote
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //默认200毫秒，
        env.getConfig().setAutoWatermarkInterval(100);

        //DataStream<Event> ds = env.fromElements(
        //                new Event("kkk", "/jkk", 1000L),
        //                new Event("ccc", "/jkl", 200L),
        //                new Event("kkk", "/jkk3", 1000L),
        //                new Event("ccc", "/jkl5", 2000L),
        //                new Event("ddd", "/jkl", 10000L),
        //                new Event("aaa", "/jkl", 100L))

        DataStream<Event> ds = env.addSource(new ClickSource())
                //乱序流的wm生成
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(300)).
                                withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                }));
        //自定义wm生成


        ds.map(new MapFunction<Event, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user,1L);
                    }
                })
                .keyBy(x -> x.f0)
                //.window(EventTimeSessionWindows.withGap(Time.seconds(1)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动事件
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5))) //
                //        .countWindow(10,2)//滑动计数
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return Tuple2.of(v1.f0,v1.f1+v2.f1);
                    }
                })
                .print()
        ;

        env.execute();


    }


}
