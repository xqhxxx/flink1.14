package sgg.flink_1_13.com.xxx.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

/**
 * @author xqh
 * @date 2022/4/22
 * @apiNote 实时统计热门url  ‘
 * 统计最近 10秒最热门的url 并且每5秒更新一次数据
 * <p>
 * 滑动窗口  访问量   排序 前2
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        //1 keyBy分组 url   统计窗口访问量
        //一定要记住 处理的数据是 数据流
        //key by 同时输出的数据 实际也是流一个一个
        SingleOutputStreamOperator<UrlViewCnt> URLds =
                ds.keyBy(x -> x.url)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                        .aggregate(new UrlCntAgg(), new UrlViewCntRs());

        URLds.print("url  cnt:");//打印的是每个窗口下：每个key（url）的cnt数量

        //2  同一窗口 访问量  收集排序
        URLds.keyBy(x -> x.windowEnd)
                .process(new TopNPF(2))
                .print();

        env.execute();
    }

    //实现自定义的key的pf  二次开窗 统计排序
    //key in out
    public static class TopNPF extends KeyedProcessFunction<Long, UrlViewCnt, String> {
        //定义属性：n
        private Integer n;
        // 定义列表状态
        private ListState<UrlViewCnt> urlViewCntListState;

        public TopNPF(Integer n) {
            this.n = n;
        }

        //在环境中获取状态  状态
        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCntListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<UrlViewCnt>("url-cont-list", Types.POJO(UrlViewCnt.class)));
        }

        @Override
        public void processElement(UrlViewCnt urlViewCnt, Context context, Collector<String> collector) throws Exception {
            //将数据存到状态中
            urlViewCntListState.add(urlViewCnt);
            //注册windowEnd+1ms的定时器
            context.timerService().registerEventTimeTimer(context.getCurrentKey()+1);
        }


        @Override
        //定时器执行：统计当前窗口的下的所有数据 然后排序top n
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCnt, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCnt> urlViewCntArrayList = new ArrayList<>();

            for (UrlViewCnt urlViewCnt : urlViewCntListState.get()) {
                urlViewCntArrayList.add(urlViewCnt);
            }
                //排序
            urlViewCntArrayList.sort((o1, o2) -> o2.count.intValue() - o1.count.intValue());

            //包装信息 打印
            StringBuffer buffer=new StringBuffer();
            buffer.append("****************\n");
            buffer.append("窗口结束时间："+new Timestamp(ctx.getCurrentKey())+"\n");

            //取list前两行
            for (int i = 0; i < 2; i++) {
                UrlViewCnt ct = urlViewCntArrayList.get(i);
                String info="No."+(i+1)+" "
                        +"url:"+ct.url+" "
                        +"访问量："+ct.count+" \n";
                buffer.append(info);
            }
            buffer.append("****************\n");

            out.collect(buffer.toString());

        }
    }

    //增量聚合 in acc out
    public static class UrlCntAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
            //窗口输入； 将结果返回 作为后面窗口的入参
         }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }

    }

    //自定义PWF 包装窗口信息
    //in  out key tw
    public static class UrlViewCntRs extends ProcessWindowFunction<Long, UrlViewCnt, String, TimeWindow> {
        @Override
        //key ctx in out
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCnt> out) throws Exception {

            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long cnt = elements.iterator().next();
            out.collect(new UrlViewCnt(url, cnt, start, end));
            //此处是针对每一个key累计打印输出的 无法放一起排序
            //统计每个url下的时间窗口访问量，需要二次开窗
        }
    }

}
