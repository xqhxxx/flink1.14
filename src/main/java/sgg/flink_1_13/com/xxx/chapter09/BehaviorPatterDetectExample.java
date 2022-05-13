package sgg.flink_1_13.com.xxx.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xqh
 * @date 2022/5/10
 * @apiNote 广播状态     动态规则
 * 行为模式检测
 */
public class BehaviorPatterDetectExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //用户行为数据流
        DataStreamSource<Action> ds1 = env.fromElements(
                new Action("a", "login"),
                new Action("a", "pay"),
                new Action("b", "login"),
                new Action("b", "order")
        );

        // 行为模式流 基于他构建广播流
        DataStreamSource<Pattern> ds2 = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );

        //定义广播状态描述器
        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = ds2.broadcast(descriptor);

        //连接流 处理
        SingleOutputStreamOperator<Tuple2<String, Pattern>> result = ds1.keyBy(x -> x.userId)
                .connect(broadcastStream)
                .process(new PatternDetector());

        result.print();

        env.execute();

    }

    //    KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>
    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        //定义一个 keyedState 保存上一次用户行为
        ValueState<String> preActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            preActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("preState", String.class));

        }

        @Override
        public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            //从广播状态汇总获取匹配规则
            ReadOnlyBroadcastState<Void, Pattern> broadcastState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern=broadcastState.get(null);

            //获取上一次的行为
            String preAct=preActionState.value();

            //判断是否匹配
            if (pattern!=null && preAct!=null){
                if (pattern.action1.equals(preAct) && pattern.action2.equals(action.action)){
                    collector.collect(new Tuple2<>(ctx.getCurrentKey(),pattern));
                }
            }

            //更新状态
            preActionState.update(action.action);

        }

        @Override
        public void processBroadcastElement(Pattern pattern, Context ctx, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            //从上下文中获取广播状态 并用当前数据更新状态
            BroadcastState<Void, Pattern> broadcastState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));

            broadcastState.put(null, pattern);

        }
    }


    //定义 用户行为事件 模式的POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

}
