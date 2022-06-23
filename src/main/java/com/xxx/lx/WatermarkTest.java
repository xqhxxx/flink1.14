package com.xxx.lx;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author xqh
 * @date 2022/6/23  08:37:44
 * @apiNote
 *
 * 比较  水位线的生成时刻 以及触发
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1);
        System.out.println(env.getConfig().getAutoWatermarkInterval());

        //模拟数据 方便观察
        DataStream<Long> ds = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Long>() {
                    @Override
                    public Long map(String value) throws Exception {
                        return Long.valueOf(value);
                    }
                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Long>forBoundedOutOfOrderness(Duration.ZERO)
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Long>() {
//                            @Override
//                            public long extractTimestamp(Long element, long recordTimestamp) {
//                                return element;
//                            }
//                        }));
                /*自定义水印  比较与process的先后  以及开窗的先后*/
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy());

        ds
                .process(new ProcessFunction<Long, Long>() {
                    @Override
                    public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                        System.out.println("单process中 事件value时间：" + value +
                                " 获取ct wm:" + ctx.timerService().currentWatermark());
                        /*
                         * 通过自定义水位线生成策略  对比 单纯的直接执行ds.process
                         *
                         * 单process中 事件value时间：4 获取ct wm:2
                         * 水印生成器中 当前事件时间：4 水位线：3
                         *
                         * 先执行的是process  currentWatermark获取的上一条数据的wm
                         * 然后再执行水位线生成器
                         * */
                        out.collect(value);
                    }
                })

//                .keyBy(x -> "true")
                .keyBy(new KeySelector<Long, String>() {
                    @Override
                    public String getKey(Long value) throws Exception {
                        return "true";
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context ctx, Iterable<Long> elements, Collector<String> out) throws Exception {
                        System.out.println("wm" + ctx.currentWatermark());
                        elements.forEach(x-> System.out.print(x+" "));

                        System.out.println("end " + ctx.window().getEnd());
                        System.out.println();
                        //1秒的翻滚窗口[左闭右开） ，比如[0ms,999ms)的数据，当1000的数据（水位线999）来的时候触发这个窗口计算结束 打印输出
                        // 这时候的水位线是999,因为：
                        // wm特点：时间戳≤水位线的数据全部到齐
                        // 这样再来一条1000的数据也能进去下一个窗口，假如这时wm是1000 那么表示的是1000的数据都到齐 后续1000并不能进来
                        // watermark= 999时（即evenTime=1000），窗口0~1000毫秒会被关闭并触发计算
                    }
                })
                .print();

        env.execute();
    }

    /**
     * 自定义水印生成策略
     */
    public static class CustomWatermarkStrategy implements WatermarkStrategy<Long> {
        @Override
        public TimestampAssigner<Long> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Long>() {
                @Override
                public long extractTimestamp(Long el, long l) {
                    //告诉程序 数据源里的时间戳是哪一个字段
                    return el;
                }
            };
        }

        @Override
        public WatermarkGenerator<Long> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessWatermarks();
            //return new PunctuatedGenerator();
        }
    }

    /**
     * 自定义 周期性 WatermarkGenerator 水印生成器
     */
    public static class CustomBoundedOutOfOrdernessWatermarks implements WatermarkGenerator<Long> {
        private Long delayTime = 0L;//延迟时间
        private Long MaxTs = -Long.MAX_VALUE + delayTime + 1L;//观察的最大时间戳
        @Override
        //按事件发送
        public void onEvent(Long event, long l, WatermarkOutput watermarkOutput) {
            //每来一条数据就调用一次
            MaxTs = Math.max(event, MaxTs); //更新最大时间戳
            long wm = MaxTs - delayTime - 1L;
            System.out.println("水印生成器中 当前事件时间：" + event + " 水位线：" + wm);
            watermarkOutput.emitWatermark(new Watermark(wm));
            //
        }

        @Override
        //周期发送
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //发射水位线 默认200ms调用一次
//            watermarkOutput.emitWatermark(new Watermark(MaxTs - delayTime - 1L));//-1ms  是窗口左闭右开

        }
    }


}


