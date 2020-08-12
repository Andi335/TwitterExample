package consoleProgramm.twitterAnalysis;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class TopHashtag {
    static List<String> user = new ArrayList<>();
    static List<Long> userIds = new ArrayList<>();
    public static void main(Properties props, List<String> userr, List<Long> userIdss) throws Exception {

        user = userr;
        userIds = userIdss;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TwitterSource twitterSource = new TwitterSource(props);
        TweetFilter customFilterInitializer = new TweetFilter();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);
        DataStream<String> source = env.addSource(twitterSource);


        DataStream<Tuple5<String, Integer, Integer, String, Long>> streamsource =

                source.flatMap(new ExtractTwitterText())
//                        .filter(new FilterHashTags())
                        .keyBy(0)
                        .timeWindow(Time.seconds(1))
                        .sum(1)
                        .timeWindowAll(Time.seconds(1))
                        .apply(new GetToHashtag());
//        streamsource.print();


        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));
//
//        streamsource.addSink(new ElasticsearchSink<>(
//                config,
//                transports,
//                new PopularHashtags()));


        env.execute();
    }


    public static class PopularHashtags
            implements ElasticsearchSinkFunction<Tuple9<String, Integer, Integer, String, String, Integer, Integer, String, Long>> {

        // construct index request
        @Override
        public void process(
                Tuple9<String, Integer, Integer, String, String, Integer, Integer, String, Long> record,
                RuntimeContext ctx,
                RequestIndexer indexer) {

            // construct JSON document to index
            Map<String, String> json = new HashMap<>();
            json.put("time", record.f8.toString());
            json.put("accName", record.f3);
            json.put("cnt", record.f1.toString());
            json.put("cntret", record.f2.toString());
            json.put("hashtag", record.f0);

            IndexRequest rqst = Requests.indexRequest()
                    .index("hashtags")        // index name
                    .type("tophashtag")  // mapping name
                    .source(json);

            indexer.add(rqst);
        }
    }


    public static class TweetFilter implements TwitterSource.EndpointInitializer, Serializable {
        @Override
        public StreamingEndpoint createEndpoint() {

            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

            endpoint.followings(userIds);
            return endpoint;
        }
    }

    private static class ExtractTwitterText implements FlatMapFunction<String, Tuple4<String, Integer, Integer, String>> {

        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void flatMap(String tweetJsonStr, Collector<Tuple4<String, Integer, Integer, String>> collector) throws Exception {
            if (!tweetJsonStr.equals("")) {

                JsonNode tweetJson = mapper.readTree(tweetJsonStr);

                JsonNode acc = tweetJson.get("user");
                if (acc == null) return;
                JsonNode name = acc.get("name");
                if (name == null) return;
                JsonNode accName = acc.get("screen_name");

                JsonNode entities = tweetJson.get("entities");
                if (entities == null) return;

                JsonNode hashtags = entities.get("hashtags");
                if (hashtags == null) return;

                if (tweetJson.get("truncated").asBoolean()) {
                    hashtags = tweetJson.get("extended_tweet").get("entities").get("hashtags");
                } else {
                    if (tweetJson.has("retweeted_status")) {
                        if (tweetJson.get("retweeted_status").get("truncated").asBoolean()) {
                            hashtags = tweetJson.get("retweeted_status").get("extended_tweet").get("entities").get("hashtags");
                        }
                    }
                }

                if (user.contains(accName.textValue())
                ) {
                    for (Iterator<JsonNode> iter = hashtags.elements(); iter.hasNext(); ) {
                        JsonNode node = iter.next();
                        String hashtag = node.get("text").textValue();
                        if (!hashtag.isEmpty() && tweetJson.has("retweeted_status")) {
                            collector.collect(new Tuple4<>(hashtag, 1, 1, name.textValue()));
                            System.out.println(tweetJsonStr);
                        }
                        if (!hashtag.isEmpty() && !tweetJson.has("retweeted_status")) {
                            collector.collect(new Tuple4<>(hashtag, 1, 0, name.textValue()));
                            System.out.println(tweetJsonStr);
                        }
                    }
                }

            }
        }
    }

    private static class GetToHashtag implements AllWindowFunction<Tuple4<String, Integer, Integer, String>,
            Tuple5<String, Integer, Integer, String, Long>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple4<String, Integer, Integer, String>> hashtags,
                          Collector<Tuple5<String, Integer, Integer, String, Long>> out) throws Exception {

            for (Tuple4<String, Integer, Integer, String> hashTag : hashtags) {

                out.collect(new Tuple5<>(hashTag.f0, hashTag.f1, hashTag.f2, hashTag.f3, window.getEnd()));
                System.out.println(hashTag.f0 + hashTag.f1 + hashTag.f2 + hashTag.f3 + window.getEnd());

            }
        }
    }
}

