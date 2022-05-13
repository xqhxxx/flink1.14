package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author xqh
 * @date 2022/4/2
 * @apiNote
 */
public class SinkToUserDef_ {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1 source
        DataStreamSource<Event> ds = env.addSource(new ClickSource());


        //写入自定义sink  继承

        //
        ds.addSink(new RichSinkFunction<Event>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //连接
            }


            @Override
            public void invoke(Event value, Context context) throws Exception {
                super.invoke(value, context);
            //   来源一次数据 写一次数据
            }
            @Override
            public void close() throws Exception {
                super.close();
            }

        });


        env.execute();


    }
}
