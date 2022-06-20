package sgg.flink_1_13.com.xxx.flinkSql_p.exampls;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xqh
 * @date 2022/6/17  16:57:53
 * @apiNote
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {

        Logger log = LoggerFactory.getLogger("Demo02");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo SQL 与 DataStream API 的转换
        // 在 pdd 这种发补贴券的场景下，希望可以在发的补贴券总金额超过 1w 元时，及时报警出来，来帮助控制预算，防止发的太多。

        // 1. pdd 发补贴券流水数据
        String createTableSql = "CREATE TABLE source_table (\n"
                + "  id BIGINT,\n"
//                -- 补贴券的流水 id
                + "  money BIGINT,\n"
//                -- 补贴券的金额
                + "  row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '100000'\n"
                + ")\n";

        // 2. 计算总计发放补贴券的金额
        String querySql = "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(money) as sum_money,\n"
//                -- 补贴券的发放总金额
                + "      count(distinct id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE source_table\n"
                + "         , DESCRIPTOR(row_time)\n"
                + "         , INTERVAL '5' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";

        tableEnv.executeSql(createTableSql);

        Table resultTable = tableEnv.sqlQuery(querySql);

        // 3. 将金额结果转为 DataStream，然后自定义超过 1w 的报警逻辑
        tableEnv.toDataStream(resultTable).flatMap(new FlatMapFunction<Row, String>() {
            @Override
            public void flatMap(Row value, Collector<String> out) throws Exception {
//                System.out.println(value.toString());
                Long sum_money = (Long) value.getField("sum_money");
                long l = sum_money;
                if (l > 1000L) {
                    log.info("报警，超过 1w");
                }
            }
        });
        env.execute();

    }
}
