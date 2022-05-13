package sgg.flink_1_13.com.xxx.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/5/9
 * @apiNote
 * 周期性计算pv 定时打印总的累加值
 * ValueState
 */
public class PeriodicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds1 = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        ds1.print("input:");
        //统计每个用户pv
        ds1.keyBy(x -> x.user)
                .process(new PerPvResult())
                .print();


        env.execute();

    }

    //
    public static class PerPvResult extends KeyedProcessFunction<String, Event, String> {

        //定义状态 保存当前pv值 以及有没有定时器
        ValueState<Long> cntState;
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            cntState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
//            更新cnt

            Long cnt = cntState.value();
            cntState.update(cnt == null ? 1 : cnt + 1);

            // ****如果没有注册过的话 注册定时器
            if (timerState.value() != null) {
                context.timerService().registerEventTimeTimer(event.timestamp + 10 * 1000L);
                timerState.update(event.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
//定时器触发 输出一次结果
            out.collect(ctx.getCurrentKey() + " pv； " + cntState.value());
//            清空状态
            timerState.clear();
            // ***直接结基于时间注册（类似于滚动窗口   ；  前面的 是基于时间重新注册（可能会有时间间隔
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerState.update(timestamp + 10 * 1000L);
        }
    }
}
