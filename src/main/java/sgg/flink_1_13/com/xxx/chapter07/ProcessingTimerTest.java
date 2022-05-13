package sgg.flink_1_13.com.xxx.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author xqh
 * @date 2022/4/22
 * @apiNote Timer定时器
 */
public class ProcessingTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource());

        ds.keyBy(x -> x.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        Long ct = context.timerService().currentProcessingTime();
                        collector.collect(context.getCurrentKey()+" 数据到达 时间：:"+new Timestamp(ct));

                        //注册一个10秒的定时器
                        context.timerService().registerProcessingTimeTimer(ct+10*1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey()+":定时器触发 的时间"+new Timestamp(timestamp));

                    }
                }).print();

        env.execute();

    }
}
