package sgg.flink_1_13.com.xxx.chapter06;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

/**
 * @author xqh
 * @date 2022/4/8
 * @apiNote
 */
public class WaterMaTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //默认200毫秒，
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<Event> ds = env.fromElements(
                        new Event("kkk", "/jkk", 1000L),
                        new Event("ccc", "/jkl", 200L),
                        new Event("kkk", "/jkk3", 1000L),
                        new Event("ccc", "/jkl5", 2000L),
                        new Event("ddd", "/jkl", 10000L),
                        new Event("aaa", "/jkl", 100L))
                //有序流的wm生成
/*                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                }));*/
                //乱序流的wm生成
/*                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(300)).
                                withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                }));*/
                //自定义wm生成
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy());


        env.execute();

        //另外 可在数据源发射水位线

    }

    /**
     * 自定义水印生成策略
     */
    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    //告诉程序 数据源里的时间戳是哪一个字段
                    return event.timestamp;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessWatermarks();
            //return new PunctuatedGenerator();
        }


    }

    /**
     * 自定义 周期性 WatermarkGenerator 水印生成器
     */
    public static class CustomBoundedOutOfOrdernessWatermarks implements WatermarkGenerator<Event> {

        private Long delayTime = 5000L;//延迟时间
        private Long MaxTs = -Long.MAX_VALUE + delayTime + 1L;//观察的最大时间戳

        @Override
        //按事件发送
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            //每来一条数据就调用一次
            MaxTs = Math.max(event.timestamp, MaxTs); //更新最大时间戳
            //
        }

        @Override
        //周期发送
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //发射水位线 默认200ms调用一次
            watermarkOutput.emitWatermark(new Watermark(MaxTs - delayTime - 1L));//-1ms  是窗口左闭右开

        }
    }

    /**
     * 自定义 断点式 WatermarkGenerator 水印生成器
     */
    public static class PunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            //只有在遇到特定的itemId时，才发出水位线
            if (event.user.equals("Mary")) {
                watermarkOutput.emitWatermark(new Watermark(event.timestamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //不做任何事 ，因为在onEvent方法发射了水位线
        }
    }
}
