package sgg.flink_1_13.com.xxx.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

/**
 * 测试 union 合并的水位线 怎么更新
 * 以最小的为准
 */
public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.socketTextStream("127.0.0.1", 7777)
                .map(x -> {
//                    a /home 1000
                    String[] arr = x.split(" ");
                    return new Event(arr[0], arr[1], Long.valueOf(arr[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        SingleOutputStreamOperator<Event> ds1 = env.socketTextStream("127.0.0.1", 8888)
                .map(x -> {
                    String[] arr = x.split(" ");
                    return new Event(arr[0], arr[1], Long.valueOf(arr[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                }));

        ds.print();
        ds1.print();

        ds.union(ds1)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect("水位线是多少：" + context.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();

    }
}
