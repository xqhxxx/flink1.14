package com

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//object Env {
//  def main(args: Array[String]): Unit = {
//
//    // 批处理
//    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    // 流处理
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//
//    //    createLocalEnvironment：返回本地执行环境，需要在调用时指定默认的并行度
//    val env_local = StreamExecutionEnvironment.createLocalEnvironment(1)
//
//    //    createRemoteEnvironment：返回集群执行环境，将Jar提交到远程服务器。需要在调用时指定JobManager的IP和端口号，并指定要在集群中运行的Jar包。
//    //    val env_remote = ExecutionEnvironment.createRemoteEnvironment("jobmanage-hostname", 6123,"YOURPATH//wordcount.jar")
//
//  }
//
//}
