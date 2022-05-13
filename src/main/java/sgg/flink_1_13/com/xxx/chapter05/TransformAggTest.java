package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote
 */
public class TransformAggTest {
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


        //按键分组  聚合 最近一次访问数据
        ds.keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.user;
                    }
                }).max("timestamp")
                .print("max:");

        ds.keyBy(x -> x.user)
                .maxBy("timestamp")
                .print("maxBy:");

        //max  只针对当前 timestamp数据更新， 其他值不变（即user url  还是原来的）
        //maxBy  整个数据更新

        env.execute();
    }
}
