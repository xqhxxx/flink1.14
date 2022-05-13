package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote 测试自定义数据源
 */
public class SourceCustomTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //DataStreamSource<Event> customStream = env.addSource(new ClickSource());
        //测试并行
        DataStreamSource<Integer> customStream = env.addSource(new ParallelCustomSource()).setParallelism(2);
                //.returns(Types.INT) 指定类型
        //returns(new TypeHint<Integer>(){});
        customStream.print();
        env.execute();

    }

    //实现自定义的并行sourceFunction
    public static class ParallelCustomSource implements ParallelSourceFunction<Integer> {

        private Boolean running = true;

        Random radom = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {

            while (running) {
                ctx.collect(radom.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
