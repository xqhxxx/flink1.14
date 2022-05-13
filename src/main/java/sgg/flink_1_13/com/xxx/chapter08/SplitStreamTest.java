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
 *
 */
public class SplitStreamTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        //定义输出标签  给侧输出流使用
        OutputTag<Tuple3<String,String,Long>> aTag=new OutputTag<Tuple3<String,String,Long>>("a"){};
        OutputTag<Tuple3<String,String,Long>> bTag=new OutputTag<Tuple3<String,String,Long>>("b"){};

        SingleOutputStreamOperator<Event> pds = ds.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {
                if (event.user.equals("a"))
                    context.output(aTag, Tuple3.of(event.user, event.url, event.timestamp));
                else if (event.user.equals("b"))
                    context.output(bTag, Tuple3.of(event.user, event.url, event.timestamp));
                else
                    collector.collect(event);
            }
        });

        pds.print("else");
        pds.getSideOutput(aTag).print("a");
        pds.getSideOutput(bTag).print("b");

        env.execute();

    }
}
