package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Properties;

/**
 * @author xqh
 * @date 2022/4/2
 * @apiNote
 */
public class SinkToRedis {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1 source
        DataStreamSource<Event> ds = env.addSource(new ClickSource());


        //写入redis
        //创建jedis连接配置
        //FlinkJedisPoolConfig config = new FlinkJedisPoolConfig
        //        .Builder()
        //        .setHost("")
        //        .build();
        //
        //ds.addSink(new RedisSink<>(config,));


        env.execute();


    }
}
