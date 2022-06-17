-- 周期内累计 PV，UV 指标在 flink 1.13 版本的最优解决方案
--
-- flink 1.13 之前 可选的解决方案有两种    retract流
-- tumble window（1天窗口） + early-fire（1分钟）   处理时间触发
-----   table.exec.emit.early-fire.enabled 开启提前触发窗口计算，并输出结果， 默认值: false
--      table.exec.emit.early-fire.delay 提前触发窗口计算的间隔时间
--         TableConfig config = tenv.getConfig();
--       config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
--       config.getConfiguration().setString("table.exec.emit.early-fire.delay","10s");
        //每10秒钟触发一次输出
-- group by（1天） + minibatch（1分钟）

-- ###########cumulate 累积窗口  append 流  事件时间触发
--例如：以天为窗口，每分钟输出一次当天零点到当前分钟的累计值  !!!!跨天注意时区问题
select unix_timestamp(cast(window_start as string))*1000 as window_start,
       window_end,
       sum(money) as sum_money,
       count(distinct id) as count_distinct_id
from table (
    cumulate(table source_table,descriptor(row_time),interval '60' second,interval '1' day)
         )
group by window_start,window_end

