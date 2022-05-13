package sgg.flink_1_13.com.xxx.chapter10;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author xqh
 * @date 2022/5/11
 * @apiNote
 */
public class CheckPointTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启检查点 周期保存
        env.enableCheckpointing(1000L);//1s触发间隔
        //设置状态后端  默认是哈希表状态
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        //配置检查点存储   JobManager堆内存
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
            //文件系统
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://"));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointTimeout(60*1000L);//超过时间直接丢掉
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
checkpointConfig.setMinPauseBetweenCheckpoints(500L);//前后两次间隔   前一次触发保存结束后+500ms（对比于1s）
        checkpointConfig.setMaxConcurrentCheckpoints(1);//最大并发检查点保存  检查点的保存时间花费太久
        checkpointConfig.enableUnalignedCheckpoints();//非对齐  必须是 EXACTLY_ONCE
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//外部检查点 取消时保留
         checkpointConfig.setTolerableCheckpointFailureNumber(0);//默认不允许失败

        //保存点配置
        env.setDefaultSavepointDirectory("hdfs:///flink/");



    }
}
