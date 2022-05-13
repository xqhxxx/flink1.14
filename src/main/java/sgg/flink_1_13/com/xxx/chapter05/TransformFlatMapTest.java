package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromElements(
                new Event("kkk", "/jkk", 1000L),
                new Event("ccc", "/jkl", 200L),
                new Event("ddd", "/jkl", 10000L),
                new Event("aaa", "/jkl", 100L)
        );


        //SingleOutputStreamOperator<String> rs = ds.flatMap((FlatMapFunction<Event, String>) (event, collector) -> {
        //    collector.collect(event.user);
        //    collector.collect(event.url);
        //    collector.collect(event.timestamp.toString());
        //});
//
        SingleOutputStreamOperator<String> rs =
                ds.flatMap((FlatMapFunction<Event, String>) (event, collector) -> {
                    if (event.user.equals("kkk")) {
                        collector.collect(event.url);
                    } else if (event.user.equals("aaa")) {
                        collector.collect(event.user);
                        collector.collect(event.url);
                        collector.collect(event.timestamp.toString());
                    }
                }).returns(new TypeHint<String>() {
                });//泛型擦除

        rs.print();
        env.execute();
    }
}
