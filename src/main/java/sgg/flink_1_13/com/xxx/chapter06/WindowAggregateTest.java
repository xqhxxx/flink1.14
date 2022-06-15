package sgg.flink_1_13.com.xxx.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author xqh
 * @date 2022/4/8
 * @apiNote 聚合函数
 */
public class WindowAggregateTest {
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
        //   aggregate  
        ds.keyBy(x -> x.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {
                    @Override
                    //累加器  初始化 赋值
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L, 0);
                    }

                    @Override
                    //状态改变
                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0 + event.timestamp, accumulator.f1 + 1);
                    }

                    @Override
                    //最终结果
                    public String getResult(Tuple2<Long, Integer> accumulator) {
                        return new Timestamp(accumulator.f0 / accumulator.f1).toString();
                    }

                    @Override
                    //合并累加器
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                }).print();

        env.execute();


    }




    //自定义一个增量  aggregateFun
    public  static  class  MyAgg implements  AggregateFunction<Event,Tuple2<Long, HashSet<String>>,Double>{

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L,new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> acc) {
            //每来一条 处理结果
            acc.f1.add(event.user);
            return Tuple2.of(acc.f0+1,acc.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> acc) {
            //窗口触发时   输出
            return Double.valueOf(acc.f0/acc.f1.size());
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }

}
