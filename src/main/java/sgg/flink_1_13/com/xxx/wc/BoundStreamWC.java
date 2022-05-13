package sgg.flink_1_13.com.xxx.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author xqh
 * @date 2022/3/18
 * @apiNote
 */
public class BoundStreamWC {
    public static void main(String[] args) throws Exception {
        //流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.readTextFile("input/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> dss2 = dss.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] arr = line.split(" ");
                    for (String word:arr){
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //分组
        //dss2.keyBy(0);
        KeyedStream<Tuple2<String, Long>, String> dss3 = dss2.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = dss3.sum(1);

        sum.print();

        //启动执行
        env.execute();

    }
}
