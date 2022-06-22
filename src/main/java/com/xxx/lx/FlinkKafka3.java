package com.xxx.lx;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/6/22  16:35:42
 * @apiNote 读取kafka3.2版本的数据
 */
public class FlinkKafka3 {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("192.168.0.4:9092")
                .setTopics("flinkTopic")
                .setGroupId("group_flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> ds = env.fromSource(kafkaSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] arr = element.split(",");
                                return Long.parseLong(arr[2]);
                            }
                        }), " kafka source");


        ds.keyBy(new KeySelector<String, Boolean>() {
                    @Override
                    public Boolean getKey(String value) throws Exception {
                        return true;
                    }
                }).process(new KeyedProcessFunction<Boolean, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        String[] arr = value.split(",");
//                        System.out.println(ctx.getCurrentKey());
                        System.out.println("当前水印:"+ctx.timerService().currentWatermark());
                        System.out.println("数据时间:"+arr[2]);
                        System.out.println("数据到达时间:"+ctx.timestamp());
                        System.out.println();
//                        当这条数据到达的时候


//                        比如 窗口是[0,5)秒
//                       上一条为4， 数据5.0来了 这回获取的水印还是3.999 当前的数据水印4.999 ，
//                             4.999 8
//                            5.0    9
//                            watermark时间 >= window_end_time；


                    }
                })
                .print();


        env.execute();

    }
}
