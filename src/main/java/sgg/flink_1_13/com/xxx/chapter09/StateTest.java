package sgg.flink_1_13.com.xxx.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/5/9
 * @apiNote
 */
public class StateTest {
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


        ds1.keyBy(x -> x.user)
                .flatMap(new MyFlatMap())
                .print();


        env.execute();
    }

    //测试 keyed state
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        //        定义状态
        ValueState<Event> myValState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myRedState;
        AggregatingState<Event, String> myAggState;



        //        本地变量 对比
        Long count = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> eventValueStateDescriptor = new ValueStateDescriptor<>("my-state", Event.class);
            myValState = getRuntimeContext().getState(eventValueStateDescriptor);

            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("my-list-state", Event.class));
            myMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Long>("my-map-state", String.class, Long.class));

//传入聚合函数
            myRedState = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<Event>("my-red-state", new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.user, value1.url, value2.timestamp);
                        }
                    }, Event.class));

            myAggState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Event, Long, String>("my-agg-state",
                            new AggregateFunction<Event, Long, String>() {
                                @Override
                                public Long createAccumulator() {
                                    return 0L;
                                }

                                @Override
                                public Long add(Event value, Long accumulator) {
                                    return accumulator + 1;
                                }

                                @Override
                                public String getResult(Long accumulator) {
                                    return "count:" + accumulator;
                                }

                                @Override
                                public Long merge(Long a, Long b) {
                                    return a + b;
                                }
                            }, Long.class));

            //2.4 配置状态的TTl   只支持 处理时间的ttl
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//什么时候更改
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)//状态的可见性 ，返回失效数据
                    .build();
            eventValueStateDescriptor.enableTimeToLive(ttlConfig);

        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //
//            System.out.println(myValState.value());
//            myValState.update(value);
//            System.out.println("my val: " + myValState.value());

            myListState.add(value);

            myMapState.put(value.user, myMapState.get(value.user) == null ? 1 : myMapState.get(value.user) + 1);
            System.out.println("my map:val:" + myMapState.get(value.user));

            myAggState.add(value);
            System.out.println("my agg:val:" + myAggState.get());

            myRedState.add(value);
            System.out.println("reduce state:" + myRedState.get());


            count++;
            System.out.println("count::" + count);
            myValState.clear();
        }
    }
}
