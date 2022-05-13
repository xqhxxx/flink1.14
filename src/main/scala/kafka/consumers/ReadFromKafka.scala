package kafka.consumers

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.Properties

object ReadFromKafka {

//  def apply(env: StreamExecutionEnvironment): Unit = {
def main(args: Array[String]): Unit = {

  //生成了配置对象
  val config = new Configuration()
  //打开flink-webui
  config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
  //获得local运行环境
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
  //    val env = StreamExecutionEnvironment.getExecutionEnvironment



    val pro: Properties = new Properties()
    pro.setProperty("bootstrap.servers", "localhost:9092")
    pro.setProperty("group.id", "consumer-group")
    pro.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("auto.offset.reset", "latest")

    // 从Kafka读取数据并换行打印
    //FlinkKafkaConsumer 1.14已弃用，将在 Flink 1.15 中移除，请KafkaSource改用
    val messageStream = env.addSource(new FlinkKafkaConsumer("kafka_test_topic", new SimpleStringSchema, pro))

    messageStream
      .flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .sum(1)
      .print()


    // 调用execute触发执行
    env.execute("Flink kafka consumer")

  }

}
