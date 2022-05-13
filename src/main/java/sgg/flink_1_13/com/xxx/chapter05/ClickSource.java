package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author xqh
 * @date 2022/3/24
 * @apiNote 自定义数据源
 */
public class ClickSource implements SourceFunction<Event> {
    //声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        //定义数据集
        String[] users = {"a", "b", "c"};
        String[] urls = {"/", "/home", "/data"};
        //循环生成数据
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            ctx.collect(new Event(user, url, Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}
