package sgg.flink_1_13.com.xxx.chapter09;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author xqh
 * @date 2022/5/9
 * @apiNote
 * MapState 映射状态
 * 模拟一个假的窗口  模拟滚动窗口
 * 例如：10s内按照url划分  统计每个url被点击的次数
 */
public class FakerWindowExample {
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


        ds.print("input:");
        ds.keyBy(x -> x.url)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();

    }

    //
    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        //定义被一个mapstate  存放每个窗口中统计的count
        MapState<Long, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("mapStateCnt", Long.class, long.class));

        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            //每来一个数据  根据时间戳 判断属于哪一个窗口（窗口分配器
            long windowStart = event.timestamp / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;

            //注册end-1的定时器
            context.timerService().registerEventTimeTimer(windowEnd - 1);

            //更新状态 进行增量聚合
            if (mapState.contains(windowStart)) {
                Long cnt = mapState.get(windowStart);
                mapState.put(windowStart, cnt + 1);
            } else
                mapState.put(windowStart, 1L);
        }

        //定时触发输出计算结果
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long cnt = mapState.get(windowStart);

            out.collect("窗口：" + new Timestamp(windowStart) + " ~" + new Timestamp(windowEnd)
                    + " url:" + ctx.getCurrentKey()
                    + " cnt:" + cnt
            );

            //模拟窗口的关闭 清除map中对应的k-v
            mapState.remove(windowStart);

        }
    }
}