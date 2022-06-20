package sgg.flink_1_13.com.xxx.flinkSql_p.exampls;

/**
 * @author xqh
 * @date 2022/6/20  10:44:35
 * @apiNote
 */
public class Demo04_time {
//    事件时间 处理时间
//    时间的作用：体现在计算（窗口、自定义） 用于标识任务的时间进度； 触发窗口计算、计算
    /*
     *指定时间属性
     *  ddl指定
     *  dataStream指定
     * */

    /*ddl
    *
  CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- 使用下面这句来将 user_action_time 声明为事件时间，并且声明 watermark 的生成规则，即 user_action_time 减 5 秒
  -- 事件时间列的字段类型必须是 TIMESTAMP 或者 TIMESTAMP_LTZ 类型
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
-- 然后就可以在窗口算子中使用 user_action_time
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
*/

    /* -- 1. 这个 ts 就是常见的毫秒级别时间戳
  ts BIGINT,
  -- 2. 将毫秒时间戳转换成 TIMESTAMP_LTZ 类型
  time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),*/

    /* -- 使用下面这句来将 user_action_time 声明为处理时间
  user_action_time AS PROCTIME()*/

}
