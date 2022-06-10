package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author xqh
 * @date 2022/4/2
 * @apiNote
 */
public class SinkToKafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1 kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "");
        DataStreamSource<String> ds =
                env.addSource(new FlinkKafkaConsumer<String>("topic",
                        new SimpleStringSchema(), properties));

        //flink1.14
        //KafkaSource kafkaSource = KafkaSource.builder()
        //        .setBootstrapServers("")
        //        .setTopics("")
        //        .setGroupId("")
        //        .build();
        //env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "s name");

        //2. transfer

        SingleOutputStreamOperator<String> ts = ds.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] arr = s.split(",");

                return arr[2] + "****" + arr[5];
            }
        });
        //3. sink
//        ts.addSink(new FlinkKafkaProducer<String>("broker ", "topic", new SimpleStringSchema()));

        ts.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("192.168.0.4:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build()
        );

        env.execute();


    }
}
