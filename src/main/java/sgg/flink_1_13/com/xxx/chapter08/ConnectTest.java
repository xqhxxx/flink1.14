package sgg.flink_1_13.com.xxx.chapter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author xqh
 * @date 2022/4/28
 * @apiNote
 *
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> ds2 = env.fromElements(4L, 5l, 6L, 7L);

        ConnectedStreams<Integer, Long> cds = ds1.connect(ds2);
        cds.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return "Integer:"+integer.toString();
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return "Long:"+aLong.toString();
            }
        }).print();

        env.execute();

    }
}
