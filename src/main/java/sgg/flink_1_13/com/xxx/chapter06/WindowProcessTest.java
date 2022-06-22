package sgg.flink_1_13.com.xxx.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @apiNote 全窗口函数
 */
public class WindowProcessTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //默认200毫秒，
        env.getConfig().setAutoWatermarkInterval(100);

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

        //数据处理计算  全部数据到达
        ds.keyBy(x -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new MyWFun())
                .print();

        env.execute();
    }

    //实现自定义的processWFun  输出一条统计信息
    public static class MyWFun extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

            //用hashSet 保存user
            HashSet<String> userSet = new HashSet<>();
            //遍历数据 去重
            for (Event ev : elements) {
                userSet.add(ev.user);
            }
            Integer uv=userSet.size();
            //结合窗口信息
            Long start=context.window().getStart();
            Long end=context.window().getEnd();
            out.collect("窗口 "+new Timestamp(start)+ "~"+new Timestamp(end)+" uv值为："+uv );

        }
    }

}
