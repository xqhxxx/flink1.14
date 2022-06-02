package sgg.flink_1_13.com.xxx.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author xqh
 * @date 2022/6/2
 * @apiNote
 */
public class LoginFailDetectProExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        1 获取登录数据
        SingleOutputStreamOperator<LoginEvent> leDS = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.1", "fail", 3000L),
                new LoginEvent("user_2", "192.168.0.1", "fail", 4000L),
                new LoginEvent("user_1", "192.168.0.1", "fail", 5000L),
//                new LoginEvent("user_2", "192.168.0.1", "success", 6000L),
                new LoginEvent("user_2", "192.168.0.1", "fail", 7000L),
                new LoginEvent("user_2", "192.168.0.1", "fail", 8000L),
                new LoginEvent("user_2", "192.168.0.1", "success", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.timestamp;
                    }
                }));

//        2 定义模式 连续三次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail") //第一次登录失败事件  泛型 标签名
                .where(new SimpleCondition<LoginEvent>() {//筛选条件
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        //返回true  表示 符合条件
                        return loginEvent.eventType.equals("fail");
                    }
                })
                //连续出现3次  类似 严格紧邻 next
                .times(3).consecutive();
//                .times(3).allowCombinations()//组合3次 类似  非确定性宽松 followedByAny



        //3 将模式应用到数据流上  检测复杂事件
        PatternStream<LoginEvent> pDS = CEP.pattern(leDS.keyBy(x -> x.userId), pattern);
        // 4 将检测到的事件 提取出来  进行处理得到报警信息
        SingleOutputStreamOperator<String> warnDS = pDS.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> match, Context ctx, Collector<String> out) throws Exception {
                LoginEvent fail1 = match.get("fail").get(0);
                LoginEvent fail2 = match.get("fail").get(1);
                LoginEvent fail3 = match.get("fail").get(2);

                out.collect(fail1.userId + " 连续三次登录失败！ 登录时间：" + fail1.timestamp
                        + "," + fail2.timestamp + "," + fail3.timestamp);

            }
        });
        warnDS.print();

        env.execute();
    }
}
