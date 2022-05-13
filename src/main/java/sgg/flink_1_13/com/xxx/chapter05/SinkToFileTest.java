package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author xqh
 * @date 2022/4/2
 * @apiNote
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> ds = env.fromElements(
                new Event("kkk", "/jkk", 1000L),
                new Event("kkk", "/jkk", 1000L),
                new Event("ccc", "/jkl", 200L),
                new Event("aaa", "/jkl", 100L),
                new Event("ccc", "/jkl", 200L),
                new Event("aaa", "/jkl", 100L)
        );

        StreamingFileSink<String> sfSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("utf-8"))
                .withRollingPolicy(//滚动策略
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))//多久没数据就
                                .build()
                )
                .build();

        ds.map(x->x.toString()).addSink(sfSink);
        env.execute();

    }
}
