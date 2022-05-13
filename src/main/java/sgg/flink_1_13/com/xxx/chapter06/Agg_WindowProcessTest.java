package sgg.flink_1_13.com.xxx.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author xqh
 * @date 2022/4/8
 * @apiNote 结合aggregate + 全窗口函数
 */
public class Agg_WindowProcessTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //默认200毫秒，
        //env.getConfig().setAutoWatermarkInterval(100);
        System.out.println(env.getConfig().getAutoWatermarkInterval());

        DataStream<Event> ds = env.addSource(new ClickSource())
                //乱序流的wm生成
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).
                                withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                }));

        //数据处理计算  使用aggregate 和ProcessWindowFunction 结合计算
        ds.keyBy(x -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .aggregate(new MyAggFun(),new MyPWFun())
                .print();

        env.execute();
    }

    //实现自定义的aggregate  计算uv值
    public static class MyAggFun implements AggregateFunction<Event, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();

        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> strings) {
            strings.add(event.user);
            return strings;
        }

        @Override
        public Long getResult(HashSet<String> strings) {
            return (long)strings.size();
            //此处数据即为 pwf的输入
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }

    //自定义PWF 包装窗口信息
    public static class MyPWFun extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {

            //用hashSet 保存user
            HashSet<String> userSet = new HashSet<>();
            //遍历数据 去重
            //for (Long ev : elements) {
            //    userSet.add(ev.user);
            //}
            Long uv =elements.iterator().next();
            //结合窗口信息
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("窗口 " + new Timestamp(start) + "~" + new Timestamp(end) + "uv值为：" + uv);
        }
    }

}
