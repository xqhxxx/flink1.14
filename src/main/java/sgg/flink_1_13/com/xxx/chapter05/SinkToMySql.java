package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/4/2
 * @apiNote
 */
public class SinkToMySql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1 source
        DataStreamSource<Event> ds = env.addSource(new ClickSource());


        //写入mysql

        //
        ds.addSink(JdbcSink.sink(
                "insert into tb_ valus(?,?)",
                ((statement, event) -> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);

                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute();


    }
}
