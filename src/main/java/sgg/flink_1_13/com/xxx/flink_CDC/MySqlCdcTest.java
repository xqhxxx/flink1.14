package sgg.flink_1_13.com.xxx.flink_CDC;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xqh
 * @date 2022/6/23  16:02:36
 * @apiNote
 */
public class MySqlCdcTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3307)
                .databaseList("cdc") // set captured database
                .tableList("cdc.flink_cdc") // set captured table
                .username("root")
                .password("root")
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        // enable checkpoint
//        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print();

        env.execute("Print MySQL Snapshot + Binlog");


    }
}
