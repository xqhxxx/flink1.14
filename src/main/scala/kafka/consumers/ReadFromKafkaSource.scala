package kafka.consumers

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util.Properties

/**
 * 消费本地kafka数据 打印到
 */
object ReadFromKafkaSource {

  def main(args: Array[String]): Unit = {

    //生成了配置对象
//    val config = new Configuration()
//    //打开flink-webui
//    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
//    //获得local运行环境
//    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
        val env = StreamExecutionEnvironment.getExecutionEnvironment

    //todo    Kafka Source 提供了一个builder类来构建KafkaSource的实例
    val kafkaSource = KafkaSource
      .builder()
      .setBootstrapServers("192.168.0.42:9092")
      .setTopics("kafka_test_topic")
      .setGroupId("my_group")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build();

    val mes = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO), "KafkaSource");

    //todo 处理数据
  val count=mes
    .flatMap(_.split(","))
        .map((_,1))
        .keyBy(_._1)
//        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .sum(1)

    //todo sink
    count.print()

    // 调用execute触发执行
    env.execute("Flink kafka -- consumer to data")

  }

}
