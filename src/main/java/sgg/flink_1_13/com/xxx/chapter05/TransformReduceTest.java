package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote
 */
public class TransformReduceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromElements(
                new Event("kkk", "/jkk", 1000L),
                new Event("ccc", "/jkl", 200L),
                new Event("kkk", "/jkk3", 1000L),
                new Event("ccc", "/jkl5", 2000L),
                new Event("ddd", "/jkl", 10000L),
                new Event("aaa", "/jkl", 100L)
        );

        //当前访问量最大
        //1统计
        SingleOutputStreamOperator<Tuple2<String, Long>>
                clickUser = ds.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(x -> x.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                });

        //2 获取活跃用户
        //所有字段都分配到相同的一个key："key"
        SingleOutputStreamOperator<Tuple2<String, Long>> rs = clickUser.keyBy(x -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return v1.f1 > v2.f1 ? v1 : v2;
                    }
                });

        rs.print();


        env.execute();
    }
}
