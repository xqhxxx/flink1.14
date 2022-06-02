package sgg.flink_1_13.com.xxx.chapter12;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author xqh
 * @date 2022/6/2  11:27:46
 * @apiNote
 */
public class NFAExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        1 获取登录数据
        KeyedStream<LoginEvent, String> ds = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.1", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.0.1", "fail", 4000L),
                        new LoginEvent("user_1", "192.168.0.1", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.0.1", "success", 6000L),
                        new LoginEvent("user_2", "192.168.0.1", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.0.1", "fail", 8000L),
                        new LoginEvent("user_2", "192.168.0.1", "success", 6000L)
                )
                .keyBy(loginEvent -> loginEvent.userId);

//        状态机  状态跳转
        SingleOutputStreamOperator<String> warnStream = ds.flatMap(new StateMachineMapper());
        warnStream.print();


        env.execute();
    }

    private static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {

        //状态机 编程
        ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent loginEvent, Collector<String> collector) throws Exception {
            //如果状态为空  进行初始化
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            //跳转下一状态

            State nextState = state.transition(loginEvent.eventType);

            //判断当前状态的特殊情况 直接进行跳转
            if (nextState == State.Matched) {
                //检测到匹配 输出报警 不更新状态 就是跳转回S2
                collector.collect(loginEvent.userId + "连续三次登录失败");
            } else if (nextState == State.Terminal) {
                //直接状态更新为初始状态 重新开始检测
                currentState.update(State.Initial);
            } else {
                //状态跳转
                currentState.update(nextState);
            }

        }


    }

    //状态机实现
    private enum State {
        Terminal,//匹配失败 终止状态
        Matched,//匹配成功

        // s2状态  传入基于s2状态可以进行的一系列状态转移
        S2(new Transition("fail", Matched), new Transition("sucess", Terminal)),

        //s1
        S1(new Transition("fail", S2), new Transition("sucess", Terminal)),

        //初始状态
        Initial(new Transition("fail", S1), new Transition("sucess", Terminal));

        private Transition[] transitions;//当前状态转移规则

        State(Transition... transitions) {
            this.transitions = transitions;
        }

        //状态转移方法
        public State transition(String eventType) {
            for (Transition transition : transitions) {
                if (transition.getEventType().equals(eventType))
                    return transition.targetState;
            }

            //回到初始状态
            return Initial;

        }


    }

    //定义状态转移类  包含当前引起状态转移的事件类型 以及转移的目标状态
    public static class Transition {
        private String eventType;
        private State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public State getTargetState() {
            return targetState;
        }

        public void setTargetState(State targetState) {
            this.targetState = targetState;
        }
    }


}
