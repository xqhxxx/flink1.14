package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

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
            Connection conn = null;
            PreparedStatement insertStmt = null;
            PreparedStatement updateStmt = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                //连接
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
                        "root", "123456");
                // 创建预编译器，有占位符，可传入参数
                insertStmt = conn.prepareStatement("INSERT INTO event (`user`, `url`) VALUES (?, ?)");
                updateStmt = conn.prepareStatement("UPDATE event SET `url` = ? WHERE `user`= ?");
            }


            @Override
            public void invoke(Event value, Context context) throws Exception {
            //   来源一次数据 写一次数据
                // 执行更新语句，注意不要留 super
                updateStmt.setString(1, value.user);
                updateStmt.setString(2, value.url);
                updateStmt.execute();
                // 如果刚才 update 语句没有更新，那么插入
                if (updateStmt.getUpdateCount() == 0) {
                    insertStmt.setString(1, value.user);
                    insertStmt.setString(2, value.url);
                    insertStmt.execute();
                }
            }
            @Override
            public void close() throws Exception {
                insertStmt.close();
                updateStmt.close();
                conn.close();
            }

        });


        env.execute();


    }
}
