import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object AppMain {
  def main(args: Array[String]): Unit = {

//    //生成了配置对象
//    val config = new Configuration()
//    //打开flink-webui
//    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
//    //配置webui的日志文件，否则打印日志到控制台，这样你的控制台就清净了
//    config.setString("web.log.path", "D:\\Logs\\Flink\\log.file")
//    //配置taskManager的日志文件，否则打印日志到控制台，这样你的控制台就清净了
//    config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "D:\\Logs\\Flink\\log.file")
//
//    //获得local运行环境
//    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
        val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并发度
    env.setParallelism(4)


    //
//    ReadFromKafka(env)

//    WordCountScala(env)

  }

}
