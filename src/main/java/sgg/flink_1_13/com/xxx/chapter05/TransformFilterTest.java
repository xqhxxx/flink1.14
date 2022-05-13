package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromElements(
                new Event("kkk", "/jkl", 1000L),
                new Event("ccc", "/jkl", 200L),
                new Event("ddd", "/jkl", 10000L),
                new Event("aaa", "/jkl", 100L)
        );


        SingleOutputStreamOperator<Event> rs = ds
                .filter(new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.user.equals("aaa");
                    }
                });

        //SingleOutputStreamOperator<Event> rs = ds.filter((FilterFunction<Event>) event -> event.user.equals("aaa"));


        rs.print();


        env.execute();


    }
}
