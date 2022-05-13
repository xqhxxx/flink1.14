package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote
 */
public class TransformMapTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> s3 = env.fromElements(
                new Event("kkk", "/jkl", 1000L),
                new Event("ccc", "/jkl", 1000L),
                new Event("ddd", "/jkl", 1000L),
                new Event("aaa", "/jkl", 1000L)
        );

        //转换计算 提取user
        //1、使用自定义类
        //SingleOutputStreamOperator<String> map = s3.map(new MyMap());
        //2、使用匿名类
        //SingleOutputStreamOperator<String> map = s3.map(new MapFunction<Event, String>() {
        //    @Override
        //    public String map(Event event) throws Exception {
        //        return event.user;
        //    }
        //});
        //3、lambda  !!!!防止泛型擦除
        SingleOutputStreamOperator<String> map =
                s3.map((MapFunction<Event, String>) event -> event.user);


        map.print();

        env.execute();


    }

//    自定义mapFunction

    public static class MyMap implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }

}
