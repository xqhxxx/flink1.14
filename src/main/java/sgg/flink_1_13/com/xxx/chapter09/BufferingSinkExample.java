package sgg.flink_1_13.com.xxx.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xqh
 * @date 2022/5/10
 * @apiNote 算子状态  将数据缓存 平均切割发送到下游
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启检查点 周期保存
        env.enableCheckpointing(1000L);
        //设置状态后端  默认是哈希表状态
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        ds.print("input::");

        //批量缓存输出
        ds.addSink(new BufferingSink(10));
        env.execute();

    }

    //
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        //
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.buffElements=new ArrayList<>();
        }

        private List<Event> buffElements;

        //定义算子状态
        private ListState<Event> checkPointState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            buffElements.add(value);//缓存到列表
//            判断达到阈值
            if (buffElements.size()==threshold){
                //打印 模拟写入外部系统
                for (Event bf : buffElements) {
                    System.out.println(bf);
                }
                System.out.println("输出完毕**");
                buffElements.clear();
            }

        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            //清空状态
            checkPointState.clear();
            //对状态进行持久化 复制缓存的列表到列表状态
            for (Event bf : buffElements) {
                checkPointState.add(bf);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            //定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("bf-element", Event.class);

            checkPointState=  ctx.getOperatorStateStore().getListState(descriptor);

            //如果是故障恢复  需将listState的数据复制到列表中
            if (ctx.isRestored()){
                for (Event event : checkPointState.get()) {
                    buffElements.add(event);
                }
            }
        }
    }
}
