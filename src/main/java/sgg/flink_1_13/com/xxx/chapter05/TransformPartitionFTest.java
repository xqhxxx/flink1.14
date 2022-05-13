package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote
 *
 * 屋里分区
 */
public class TransformPartitionFTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromElements(
                new Event("kkk", "/jkk", 1000L),
                new Event("kkk", "/jkk", 1000L),
                new Event("ccc", "/jkl", 200L),
                new Event("aaa", "/jkl", 100L),
                new Event("ccc", "/jkl", 200L),
                new Event("aaa", "/jkl", 100L)
        );


        ds.print().setParallelism(3);
        //随机分区
        ds.shuffle().print().setParallelism(2);

        //轮询分区
        //ds.rebalance().print().setParallelism(3);

        //3 重缩放分区
        //ds.rescale();

        //4  广播 每条被处理多次  --给每个分区
        //ds.broadcast().print().setParallelism(4);

        //5 全局分区  分配到一个分区
        //ds.global().print();

        //6 自定义重分区
        //env.fromElements(1,2,3,4,5,6,7,8)
        //        .partitionCustom(new Partitioner<Integer>() {
        //            @Override
        //            public int partition(Integer o, int i) {
        //                //0号、1号分区
        //                return o % 2;
        //            }
        //        }, new KeySelector<Integer, Integer>() {
        //            @Override
        //            public Integer getKey(Integer integer) throws Exception {
        //                return integer;
        //            }
        //        }).print().setParallelism(4);


        env.execute();
    }
}
