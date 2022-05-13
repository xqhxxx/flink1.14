package sgg.flink_1_13.com.xxx.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author xqh
 * @date 2022/5/9
 * @apiNote AggregatingState 聚合统计
 */
public class AverageTimestampExample {
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
        //todo 自定义实现平均时间戳的统计
        ds.keyBy(x -> x.user)
                .flatMap(new AvgTsResult(5L))
                .print();
        env.execute();

    }

    //自定义富函数
    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        private Long count;
        public AvgTsResult(Long count) {
            this.count = count;
        }
        //定义一个 聚合状态 保存平均时间戳
        AggregatingState<Event, Long> aggState;

        //值状态  保存用户访问次数
        ValueState<Long> cntState;

        @Override
        public void open(Configuration parameters) throws Exception {
            aggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "aggState",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));
            cntState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cntS", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //每来一个数据 current Cnt+1
            Long currentCnt = cntState.value();
            if (currentCnt == null) {
                currentCnt = 1L;
            } else
                currentCnt++;

            //更新状态
            cntState.update(currentCnt);
            aggState.add(value);

            //如果达到cnt次数 就会输出结果
            if (currentCnt.equals(count)) {
                out.collect(value.user + " 过去 " + count + "次 访问平均时间戳为：" + new Timestamp(aggState.get()));
                //清理状态 重新计数
                cntState.clear();//不清理的话 就是过去所有的平均值  每五次打印
                aggState.clear();
            }
        }
    }
}