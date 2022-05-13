批流一体  

执行时  改变运行模式  加上execution.runtime-mode=BATCH
bin/flink   run -Dexecution.runtime-mode=BATCH

代码设置  env.setRuntimeMode（BATCH）  不建议