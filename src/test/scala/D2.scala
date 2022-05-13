/**
 * @author xqh
 * @date 2022/3/17
 * @apiNote
 *
 */

import org.apache.flink.api.scala.{ExecutionEnvironment, _} // 加载隐式转换

object Demo02 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val ds: DataSet[Int] = env.fromElements(1, 2, 3, 4, 5,6)

    ds.setParallelism(3).print()

    ds.map(x=>x+"kk").setParallelism(2).print()


  }
}