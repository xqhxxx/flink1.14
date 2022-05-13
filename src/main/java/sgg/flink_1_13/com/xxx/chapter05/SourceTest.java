package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author xqh
 * @date 2022/3/18
 * @apiNote
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dss = env.readTextFile("input/click.txt");

/*        //2从集合读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        DataStreamSource<Integer> numsDSS = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("kkk", "/jkl", 1000L));
        DataStreamSource<Event> eventDSS = env.fromCollection(events);

        //3从元素读取数据
        DataStreamSource<Event> s3 = env.fromElements(
                new Event("kkk", "/jkl", 1000L),
                new Event("kkk", "/jkl", 1000L)
        );

        //4 从socket文本流读取
        env.socketTextStream("localhost",7777);

        dss.print("1");
        numsDSS.print("nums");
        eventDSS.print("2");
        s3.print();*/

        //5 kafka
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "localhost:9092");
        pro.setProperty("group.id", "consumer-group");
        pro.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty("auto.offset.reset", "latest");

        DataStreamSource ks = env.addSource(new FlinkKafkaConsumer("kafka_test_topic", new SimpleStringSchema(), pro));
        ks.print();


        //flink1.14 新的
        //todo    Kafka Source 提供了一个builder类来构建KafkaSource的实例
/*
        KafkaSource<String> kafkaSource = KafkaSource
                .builder()
                .setBootstrapServers("192.168.0.42:9092")
                .setTopics("kafka_test_topic")
                .setGroupId("my_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(SimpleStringSchema())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
*/




        env.execute();


    }
}
