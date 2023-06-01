package com.xxx.lx;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class KafkaJsonOrderingExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("kafka_test_topic")
                .setGroupId("group_flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //source

        // 从Kafka中读取数据流
        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "source");
//        DataStream<String> kafkaStream = env.addSource(kafkaSource);

//        kafkaStream.print();
        // 解析JSON数据并提取时间戳
        DataStream<List<JsonPoint>> orderedStream = kafkaStream
                .flatMap(new JsonKeyValueMapper())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<Long, JsonPoint>>forBoundedOutOfOrderness(Duration.ofMinutes(1)).withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<Long, JsonPoint>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<Long, JsonPoint> element, long recordTimestamp) {
                                        return element.f0;
                                    }
                                })
                )
//                .assignTimestampsAndWatermarks(new TimestampExtractor())
                .keyBy(tuple -> tuple.f1.getP())
                .process(new OrderListPointProcess());

        // 输出测点集合
//        orderedStream.map(l -> "此刻size：" + l.size() + l.get(0).getP() + " 时刻：" + l.get(0).getT() + "本地：" + LocalDateTime.now())
//                .print();

        orderedStream.filter(new FilterFunction<List<JsonPoint>>() {
            @Override
            public boolean filter(List<JsonPoint> value) throws Exception {
                boolean flag=false;
                for (JsonPoint jsonPoint : value) {
                    if (Objects.equals(jsonPoint.getP(), "p1")){
                        flag=true;
                    }else
                        flag= false;
                }
                return flag;
            }
        }).print();
        // todo 写入麦杰


        // 执行任务
        env.execute("Kafka JSON Ordering");
    }

    // 解析Json Key-Value数据，提取时间戳字段
    public static class JsonKeyValueMapper extends RichFlatMapFunction<String, Tuple2<Long, JsonPoint>> {
        String format = "yyyy-MM-dd HH:mm:ss.SSS";

        @Override
        public void flatMap(String value, Collector<Tuple2<Long, JsonPoint>> out) throws Exception {
            //todo 解析
            JSONArray jsonArray = JSONArray.parseArray(value);
            int dataSize = jsonArray.size();
            for (int i = 0; i < dataSize; i++) {
                JSONObject jo = jsonArray.getJSONObject(i);
                if (jo.size() != 4) {
                    continue;
                }
                JsonPoint jp = JSONObject.parseObject(jo.toString(), JsonPoint.class);
                if (Objects.equals(jp.getV().trim(), "false")) {
                    continue;
                }

                LocalDateTime dateTime = LocalDateTime.parse(jp.getT(), DateTimeFormatter.ofPattern(format));
                long timestamp = dateTime.toInstant(ZoneOffset.of("+0")).toEpochMilli();
                out.collect(new Tuple2<>(timestamp, jp));
            }
        }
    }

    // 提取时间戳并添加水印
    public static class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, JsonPoint>> {
        public TimestampExtractor() {
            super(Time.minutes(10));
        }

        @Override
        public long extractTimestamp(Tuple2<Long, JsonPoint> element) {
            System.out.println("水印" + element.f0);
            return element.f0;
        }
    }

    // 根据时间戳顺序 输出多个数据
    public static class OrderListPointProcess extends ProcessFunction<Tuple2<Long, JsonPoint>, List<JsonPoint>> {
        long delay_ms = 5 * 1000;//延迟  后处理
        private TreeMap<Long, List<JsonPoint>> treeMap;
        //同一时刻有多个测点数据

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            treeMap = new TreeMap<>();
        }

        @Override
        public void processElement(Tuple2<Long, JsonPoint> element, Context ctx, Collector<List<JsonPoint>> out) throws Exception {
            long timestamp = element.f0;
            JsonPoint jsonPoint = element.f1;
            if (treeMap.containsKey(timestamp)) {
                treeMap.get(timestamp).add(jsonPoint);
            } else {
                ArrayList<JsonPoint> list = new ArrayList<>();
                list.add(jsonPoint);
                treeMap.put(timestamp, list);
            }

            // 定时器触发，延迟1分钟后处理
            ctx.timerService().registerEventTimeTimer(timestamp + delay_ms);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<JsonPoint>> out) throws Exception {
            if (!treeMap.isEmpty()) {
                List list = treeMap.pollFirstEntry().getValue();
//                String  s="本次时间:"+;
                out.collect(list);

                // 输出数据，继续处理下一个
                if (!treeMap.isEmpty()) {
                    long nextTimestamp = treeMap.firstKey();
                    ctx.timerService().registerEventTimeTimer(nextTimestamp + delay_ms);
                }
            }
        }

    }

}
