package sgg.flink_1_13.com.xxx.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/4/28
 * @apiNote 对账 案例  测试connect stream
 */
public class BillCheckExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //来自app的支付订单日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ds1 = env.fromElements(
                        Tuple3.of("o-1", "app", 1000L),
                        Tuple3.of("o-2", "app", 2000L),
                        Tuple3.of("o-3", "app", 3500L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> el, long l) {
                                return el.f2;
                            }
                        }));

        //来自第三方支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> ds2 = env.fromElements(
                        Tuple4.of("o-1", "third_p", "ok", 3000L),
                        Tuple4.of("o-3", "third_p", "ok", 4000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> el, long l) {
                                return el.f3;
                            }
                        }));


        //检测同一支付单在两条流是否匹配，不匹配报警

        //先key 在连接 ;和先连接 在指定key  都可以
//        ds1.keyBy(x -> x.f0).connect(ds2.keyBy(x -> x.f0));
        //类似于a join b on a.k=b.k
        ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> cs = ds1.connect(ds2);
        cs.keyBy(x -> x.f0, x -> x.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    /**
     * 自定义 实现coPF
     * 处理连接流的处理
     */
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>,
            Tuple4<String, String, String, Long>, String> {
        //需要保存状态
        //定义状态变量  保存已经到达的事件的状态
        private ValueState<Tuple3<String, String, Long>> appState;
        private ValueState<Tuple4<String, String, String, Long>> thState;

        @Override
        public void open(Configuration parameters) {
            //初始化状态变量
            appState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("th-event",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> v1, Context context, Collector<String> collector) throws Exception {
//            来的app event  看另一条流事件是否来过
            if (thState.value() != null) {
                collector.collect("成功：：：" + v1 + " " + thState.value());
                //清空状态
                thState.clear();
            } else {
                //更新状态
                appState.update(v1);
                //注册一个5s定时器  开始等待另一条流的事件
                context.timerService().registerEventTimeTimer(v1.f2 + 5000);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> v1, Context context, Collector<String> collector) throws Exception {
            if (appState.value() != null) {
                collector.collect("成功：：：" + appState.value() + " " + v1);
                //清空状态
                appState.clear();
            } else {
                //更新状态
                thState.update(v1);
                //注册一个5s定时器  开始等待另一条流的事件
                context.timerService().registerEventTimeTimer(v1.f3 + 5000);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时触发  判断状态： 如果某个不为空 说明另一个没有来
            //  ！！！不可能同时不为空 两个进来一定有一个先后顺序触发，
            //  ！！！不可能同时为空 假如为空：就不会触发process，也就不会注册定时器
            if (appState.value() != null) {
                out.collect("对账 失败" + appState.value() + " 第三方信息未到");
            }
            if (thState.value() != null) {
                out.collect("对账 失败" + thState.value() + " app信息未到");
            }

            appState.clear();
            thState.clear();
        }
    }


}
