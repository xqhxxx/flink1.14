package sgg.flink_1_13.com.xxx.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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
import java.util.Comparator;
import java.util.HashMap;

/**
 * @author xqh
 * @date 2022/4/22
 * @apiNote 实时统计热门url  ‘
 * 统计最近 10秒最热门的url 并且每5秒更新一次数据
 * 滑动窗口  访问量   排序 前2
 * ---用全窗口函数
 */
public class TopNExample_ProcessAllWindowFun {
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


        //直接开窗  收集所有数据 排序
        ds.map(x -> x.user)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCntAgg(), new UrlAllWRes())
                .print();

        env.execute();

    }

    //增量聚合
    public static class UrlHashMapCntAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }
        @Override
        public HashMap<String, Long> add(String s, HashMap<String, Long> acc) {
            if (acc.containsKey(s)) {
                Long cnt = acc.get(s);
                acc.put(s, cnt + 1);
            } else {
                acc.put(s, 1L);
            }
            return acc;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> acc) {

            ArrayList<Tuple2<String, Long>> res = new ArrayList<>();
            for (String key : acc.keySet()) {
                res.add(Tuple2.of(key, acc.get(key)));
            }
            res.sort((o1, o2) -> o2.f1.intValue() - o1.f1.intValue());

            return res;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }


    }

    //自定义PWF 包装窗口信息
    //in  out key
    public static class UrlAllWRes extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {
        //key ctx in out
        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Long>>> iterable, Collector<String> collector) throws Exception {
            ArrayList<Tuple2<String, Long>> list = iterable.iterator().next();

            StringBuffer buffer=new StringBuffer();
            buffer.append("****************\n");
            buffer.append("窗口结束时间："+new Timestamp(context.window().getEnd())+"\n");

            //取list前两行
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> ct = list.get(i);
                String info="No."+(i+1)+" "
                        +"url:"+ct.f0+" "
                        +"访问量："+ct.f1+" \n";
                buffer.append(info);
            }
            buffer.append("****************\n");

            collector.collect(buffer.toString());
        }
    }

}
