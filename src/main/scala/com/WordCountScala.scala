package com

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object WordCountScala {
  def main(args: Array[String]): Unit = {

    //生成了配置对象
    val config = new Configuration()
    //打开flink-webui
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
//val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setMaxParallelism(16)

    //定义socket的source源
    val text: DataStream[String] = env.socketTextStream(hostname = "192.168.0.4", port = 6666)

    //scala开发需要加一行隐式转换，否则在调用operator的时候会报错，作用是找到scala类型的TypeInformation
    import org.apache.flink.api.scala._

    //定义operators，作用是解析数据，分组，并且求wordCount
    val wordCount: DataStream[(String, Int)] = text
      .flatMap(_.split(" "))
      .setParallelism(4)
      .map((_, 1))
      .setParallelism(4)
      .keyBy(_._1)
      .sum(position = 1)
      .setParallelism(4)

    //定义sink，打印数据到控制台
    wordCount.print()
      .setParallelism(4)

    //定义任务的名称并运行
    //注意：operator是惰性的，只有遇到execute才执行
    env.execute("SocketWordCount")

  }
}