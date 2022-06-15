package sgg.flink_1_13.com.xxx.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/5/9
 * @apiNote
 * ListState
 * 两条流join
 * 列表状态 全外连接
 *
 */
public class TwoStreamJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> ds1 = env.fromElements(
                Tuple3.of("a", "s1", 1000L),
                Tuple3.of("b", "s1", 2000L),
                Tuple3.of("a", "s1", 3000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> ds2 = env.fromElements(
                Tuple3.of("a", "s2", 3000L),
                Tuple3.of("b", "s2", 4000L),
                Tuple3.of("a", "s2", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        //todo  列表状态 全外连接
        ds1.keyBy(x -> x.f0).connect(ds2.keyBy(x -> x.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    //定义列表状态  保留已经到达的数据

                    private ListState<Tuple2<String, Long>> ls1;
                    private ListState<Tuple2<String, Long>> ls2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ls1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>
                                ("ls1-", Types.TUPLE(Types.STRING, Types.LONG)));
                        ls2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>
                                ("ls2-", Types.TUPLE(Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context context, Collector<String> collector) throws Exception {
                        //获取另一流中所有数据 配对输出
                        for (Tuple2<String, Long> right : ls2.get()) {
                            collector.collect(left.f0 + "**" + left.f2 + " => " + right);
                        }
                        ls1.add(Tuple2.of(left.f0, left.f2));
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context context, Collector<String> collector) throws Exception {
                        for (Tuple2<String, Long> left : ls1.get()) {
                            collector.collect(left + "=> " + right.f0 + "**" + right.f2);
                        }
                        ls2.add(Tuple2.of(right.f0, right.f2));
                    }
                }).print();


        env.execute();

    }

}
