/*
/**
 * @author xqh
 * @date 2022/3/17
 * @apiNote
 *
 */
import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}  // 加载隐式转换

object D1 {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val jsonFilePath = "D:\\works\\jxtech\\project_code\\sx\\sxjsonfile\\jsonFile"


    val jsonDF: DataSet[String] = env.readTextFile(jsonFilePath + "/" + "20210605")

    val df2: DataSet[String] = jsonDF.map(_.replace("][", ","))

      df2.map(
        x=>{
          val arr: JSONArray = JSON.parseArray(x)
          arr
        }
      )


  }
}*/
