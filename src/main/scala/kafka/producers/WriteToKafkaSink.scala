package kafka.producers

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


/**
 * 写数据到kafka数据
 */
object WriteToKafkaSink {
  def main(args: Array[String]): Unit = {

    //生成了配置对象
    val config = new Configuration()
    //打开flink-webui
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
//    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
        val env = StreamExecutionEnvironment.getExecutionEnvironment

    //todo 定义socket的source源
//    val text: DataStream[String] = env.socketTextStream(hostname = "localhost", port = 6666)
    val text: DataStream[String] = env.readTextFile("D:\\works\\jxtech\\project_code\\sx\\0809sxData\\kafka-data.txt")

    import org.apache.flink.api.scala._

    //todo 处理数据
    val mes: DataStream[String] = text
//      .flatMap(_.split(" "))
//      .map((_, 1))
//      .keyBy(_._1)
//      .sum(position = 1)

    //todo sink
    val kafkaSink = KafkaSink
      .builder()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema
          .builder()
          .setTopic("kafka_test_topic")
          .setValueSerializationSchema(new SimpleStringSchema())
//                  .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
          .build()
      )
      .build()
    println("发送数据："+mes)
 //FlinkKafkaProducer 1.14已弃用，将在 Flink 1.15 中移除，请改用KafkaSink
//    mes.addSink(new FlinkKafkaProducer[String]("hadoop02:9092", "sinkDemo", new SimpleStringSchema())) //写入kafka sink
    mes.sinkTo(kafkaSink)

    // 调用execute触发执行
    env.execute("Flink kafka - data to producer ")

  }

}
