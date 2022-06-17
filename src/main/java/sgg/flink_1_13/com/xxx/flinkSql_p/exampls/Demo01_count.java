package sgg.flink_1_13.com.xxx.flinkSql_p.exampls;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author xqh
 * @date 2022/6/17  16:34:19
 * @apiNote
 *
 */
public class Demo01_count {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 计算每一种商品（sku_id 唯一标识）的售出个数、总销售额、平均销售额、最低价、最高价

        //创建数据源表   flink自带的随机mock数据的数据源
        String sourceSql=" create table source_table(" +
                "id string," +
                "price bigint) " +
                "with (" +
                "'connector'='datagen', "
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.length' = '1',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '10000'\n"
                + ")";
        //输出汇
        String sinkSql="CREATE TABLE sink_table (\n"
                + "    id STRING,\n"
                + "    count_result BIGINT,\n"
                + "    sum_result BIGINT,\n"
                + "    avg_result DOUBLE,\n"
                + "    min_result BIGINT,\n"
                + "    max_result BIGINT,\n"
                + "    PRIMARY KEY (`id`) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
//                + "  'connector' = 'upsert-kafka',\n"
//                + "  'topic' = 'tuzisir',\n"
//                + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
//                + "  'key.format' = 'json',\n"
//                + "  'value.format' = 'json'\n"
                + ")";

        // 3. 执行一段 group by 的聚合 SQL 查询
        String selectWhereSql = "insert into sink_table\n"
                + "select id,\n"
                + "       count(*) as count_result,\n"
                + "       sum(price) as sum_result,\n"
                + "       avg(price) as avg_result,\n"
                + "       min(price) as min_result,\n"
                + "       max(price) as max_result\n"
                + "from source_table\n"
                + "group by id";

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(selectWhereSql);

    }
}
