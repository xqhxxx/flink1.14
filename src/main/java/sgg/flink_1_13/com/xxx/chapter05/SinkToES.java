package sgg.flink_1_13.com.xxx.chapter05;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author xqh
 * @date 2022/4/2
 * @apiNote
 */
public class SinkToES {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1 source
        DataStreamSource<Event> ds = env.addSource(new ClickSource());


        //写入es
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));
        ds.addSink( new ElasticsearchSink.Builder<Event>(httpHosts, new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> data = new HashMap<>();
                data.put("user",event.user);
                data.put("url", event.url);

                IndexRequest indexRequest = Requests.indexRequest()
                        .index("event")
                        .type("readingData")
                        .source(data);
                requestIndexer.add(indexRequest);
            }
        }).build());


        env.execute();


    }
}
