package com.xxx.lx;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

/**
 * @author xqh
 * @date 2023-06-01  09:59:27
 * @apiNote
 */
public class KafkaProducerRoundJsonData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<String> ds = env.addSource(new SimulateJson());
        //模拟JSON数据

//        ds.print();
        // 创建FlinkKafkaProducer.Builder
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("kafka_test_topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
//                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                .build()
                )
                .build();

        ds.sinkTo(sink);

        env.execute();
    }
}
