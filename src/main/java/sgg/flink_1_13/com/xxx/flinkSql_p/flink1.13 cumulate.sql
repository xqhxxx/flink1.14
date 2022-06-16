-- 周期内累计 PV，UV 指标在 flink 1.13 版本的最优解决方案
--
-- flink 1.13 之前 可选的解决方案有两种
-- tumble window（1天窗口） + early-fire（1分钟）
-- group by（1天） + minibatch（1分钟）

