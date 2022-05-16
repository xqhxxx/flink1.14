package sgg.flink_1_13.com.xxx.chapter11;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sgg.flink_1_13.com.xxx.chapter05.ClickSource;
import sgg.flink_1_13.com.xxx.chapter05.Event;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author xqh
 * @date 2022/5/16
 * @apiNote 表环境
 */
public class CommonApiTest {
    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        //创建表执行环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1定义环境配置来创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        //1.1 基于老版本planner进行处理
//        EnvironmentSettings settings1 = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .useOldPlanner()
//                .build();

        //1.2老版本批处理 批处理环境
        //1.3blink 批处理


        //2 表的创建
        String creatDDL = "create table click_t(" +
                " user_name string," +
                " url string," +
                " ts BIGINT" +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/click.csv', " +
                " 'format' = 'csv'" +
                ") ";
        tableEnv.executeSql(creatDDL);

        //api 调用查询
        Table clickTable = tableEnv.from("click_t");
        Table resTable = clickTable.where($("user_name").isEqual("bbb"))
                .select($("user_name"), $("url"));

        tableEnv.createTemporaryView("res_t", resTable);

        //执行sql 查询表
        Table t2 = tableEnv.sqlQuery("select user_name,url from res_t");


        // 创建输出的表
        String creatOutDDL = "create table out_t(" +
                " user_name string," +
                " url string " +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'output', " +
                " 'format' = 'csv'" +
                ") ";
        tableEnv.executeSql(creatOutDDL);
    //输出表
        t2.executeInsert("out_t");

    }
}
