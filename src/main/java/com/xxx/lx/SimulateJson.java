package com.xxx.lx;

/**
 * @author xqh
 * @date 2023-05-31  17:34:15
 * @apiNote
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SimulateJson extends RichSourceFunction<String> {
    private boolean running;
    String orignData = "[{\"p\":\"p1\",\"t\":\"t1\",\"v\":\"-48.9\",\"q\":\"1\"},{\"p\":\"p2\",\"q\":\"0\",\"t\":\"t3\",\"v\":\"310.082\"},{\"p\":\"p3\",\"q\":\"0\",\"t\":\"t2\",\"v\":\"310.055\"}]";

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        running = true;
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        while (running) {
            LocalDateTime now = LocalDateTime.now();

            List<JsonPoint> jsonPoints = JSON.parseArray(orignData, JsonPoint.class);

            // 遍历列表，替换指定位置的p值
            for (JsonPoint jsonPoint : jsonPoints) {
                Random random = new Random();
                if (jsonPoint.getP().startsWith("p")) {
                    int randomNumber = random.nextInt(10) + 1;
                    jsonPoint.setP("p" + randomNumber);

                }
                int num = random.nextInt(15 * 60 * 1000) + 1;//10min
                //速记加减时间
                LocalDateTime time;
                time = now.minusNanos(num * 1000 * 1000);
                jsonPoint.setT(time.format(df));

            }

            // 转换回JSON字符串
            String updatedData = JSON.toJSONString(jsonPoints);
            System.out.println("send:"+updatedData);
            ctx.collect(updatedData);
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {
    }
}
