package sgg.flink_1_13.com.xxx.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 2022-05-31
 */
public class LoginFailDetectExample {
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
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first") //第一次登录失败事件  泛型 标签名
                .where(new SimpleCondition<LoginEvent>() {//筛选条件
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        //返回true  表示 符合条件
                        return loginEvent.eventType.equals("fail");
                    }
                })
//                .times(3).consecutive()//连续出现3次  类似 严格紧邻 next
//                .times(3).allowCombinations()//组合3次 类似  非确定性宽松 followedByAny
                .next("second")//紧跟的第二次登陆失败
                .where(new SimpleCondition<LoginEvent>() {//筛选条件
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third")//紧跟的第三次登陆失败
                .where(new SimpleCondition<LoginEvent>() {//筛选条件
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                });

        //3 将模式应用到数据流上  检测复杂事件
        PatternStream<LoginEvent> pDS = CEP.pattern(leDS.keyBy(x -> x.userId), pattern);
        // 4 将检测到的事件 提取出来  进行处理得到报警信息
        SingleOutputStreamOperator<String> warnDS = pDS.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                //这里的key 是前面的命名  list是因为有重复发生事件

                //提取复杂事件的三次登录事件
                LoginEvent first = map.get("first").get(0);
                LoginEvent second = map.get("second").get(0);
                LoginEvent third = map.get("third").get(0);

                return first.userId + " 连续三次登录失败！ 登录时间：" + first.timestamp
                        + "," + second.timestamp + "," + third.timestamp;
            }
        });

        warnDS.print();

        env.execute();
    }
}
