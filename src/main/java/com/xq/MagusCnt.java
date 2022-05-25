/*
package com.xq;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

*/
/**
 * @author xqh
 * @date 2022/4/15
 * @apiNote
 *//*

public class MagusCnt {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //source
        DataStream<StringBuffer> ds = env.addSource(new MagusSource());
//        env.fromSource()

        //ds.print();
        ds.flatMap(new FlatMapFunction<StringBuffer, String>() {
                    @Override
                    public void flatMap(StringBuffer stringBuffer, Collector<String> collector) throws Exception {
                        String[] split = stringBuffer.toString().split("\n");

                        for (String s : split) {
                            collector.collect(s);
                        }

                    }
                })
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of("s",1);

                    }
                })
                .keyBy(x -> true)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum(1)
                .print();

        env.execute();

    }
}
*/
