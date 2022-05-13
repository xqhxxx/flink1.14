package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote
 */
public class TransformRichFTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromElements(
                new Event("kkk", "/jkk", 1000L),
                new Event("ccc", "/jkl", 200L),
                new Event("aaa", "/jkl", 100L)
        );

        ds.map(new RichMapFunction<Event, String>() {
            //富函数  open close  只执行一次； 并行度2 就分别计算执行一次

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("open 生命周期:" + getRuntimeContext().getIndexOfThisSubtask());
            }


            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close 生命周期:" + getRuntimeContext().getIndexOfThisSubtask());
            }


        }).setParallelism(2).print();


        env.execute();
    }
}
