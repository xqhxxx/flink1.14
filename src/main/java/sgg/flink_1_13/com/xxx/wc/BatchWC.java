package sgg.flink_1_13.com.xxx.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author xqh
 * @date 2022/3/17
 * @apiNote
 *
 * 还是经典的dataSet api的
 * 软弃用了
 */
public class BatchWC {
    public static void main(String[] args) throws Exception {
        //1
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2
        DataSource<String> ds = env.readTextFile("input/words.txt");
        //3
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = ds.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String w : words) {
                        out.collect(Tuple2.of(w, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        //5分组统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        //6
        sum.print();

    }

}
