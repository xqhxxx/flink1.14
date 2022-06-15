package sgg.flink_1_13.com.xxx.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
public class EvevtTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new CusSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        //时间时间定时器
        ds.keyBy(x -> x.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        Long ct = context.timestamp();
                        collector.collect(context.getCurrentKey() + " 数据到达 时间：:" + new Timestamp(ct)+" wm: "+context.timerService().currentWatermark());

                        //注册一个10秒的定时器
                        context.timerService().registerEventTimeTimer(ct + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + ":定时器触发 的时间" + new Timestamp(timestamp)+" wm: "+ctx.timerService().currentWatermark());

                    }
                }).print();

        env.execute();

    }

    ///自定义测试数据
    public static  class  CusSource implements SourceFunction<Event>{
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
        //    直接发出测试数据
            ctx.collect(new Event("Mary","/home",1000L));

            Thread.sleep(5000);
            ctx.collect(new Event("Acc","/home",11000L));
            Thread.sleep(5000);

            ctx.collect(new Event("ccc","/home",11001L));
            Thread.sleep(5000);
        }

        @Override
        public void cancel() {

        }
    }
}

/*
* 1s数据到达时刻  但此刻wm未更新 还是初始值 wm:- 然后数据处理完 更新wm:：999
* 11s数据到达 wm：999，数据处理完，wm：10999
* 11.001s数据到达 wm：10999 数据处理完，wm：11000
* 然后触达定时器（1+10S）：
*
* 后续无数据流输入，flink会将WM推到Long的最大值，
* 导致后面所有的定时器触发
*
Mary 数据到达 时间：:1970-01-01 08:00:01.0 wm: -9223372036854775808
Acc 数据到达 时间：:1970-01-01 08:00:11.0 wm: 999
Acc 数据到达 时间：:1970-01-01 08:00:11.001 wm: 10999
Mary:定时器触发 的时间1970-01-01 08:00:11.0 wm: 11000
Acc:定时器触发 的时间1970-01-01 08:00:21.0 wm: 9223372036854775807
Acc:定时器触发 的时间1970-01-01 08:00:21.001 wm: 9223372036854775807
* */