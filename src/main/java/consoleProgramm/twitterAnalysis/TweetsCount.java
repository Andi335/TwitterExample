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
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class TweetsCount {
    static List<String> words = new ArrayList<>();
    static List<String> user = new ArrayList<>();
    static List<Long> userIds = new ArrayList<>();

    public static void main(Properties props, List<String> userr, List<String> wordss, List<Long> userIdss) throws Exception {

        user = userr;
        words = wordss;
        userIds = userIdss;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TwitterSource twitterSource = new TwitterSource(props);
        TweetFilter customFilterInitializer = new TweetFilter();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);

        DataStream<String> streamSource = env.addSource(twitterSource);


        DataStream<Tuple5<Long, Integer, Integer, Integer, String>> source =

                streamSource.flatMap(new ExtractTwitterText())
                        .keyBy(0)
                        .timeWindow(Time.milliseconds(500))
                        .sum(1)
                        .timeWindowAll(Time.seconds(1))
                        .apply(new GetTopTweet());

        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

        source.addSink(new ElasticsearchSink<>(
                config,
                transports,
                new TweetCountInserter()));

        env.execute();
    }

    public static class TweetCountInserter
            implements ElasticsearchSinkFunction<Tuple5<Long, Integer, Integer, Integer, String>> {

        // construct index request
        @Override
        public void process(
                Tuple5<Long, Integer, Integer, Integer, String> record,
                RuntimeContext ctx,
                RequestIndexer indexer) {

            // construct JSON document to index
            Map<String, String> json = new HashMap<>();
            json.put("time", record.f0.toString());// timestamp
            json.put("cnttweet", record.f1.toString());
            json.put("cntret", record.f2.toString());
            json.put("cntword", record.f3.toString());
            json.put("accName", record.f4);

            IndexRequest rqst = Requests.indexRequest()

                    .index("tweets")        // index name
                    .type("wordcount")  // mapping name
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

    private static class ExtractTwitterText implements FlatMapFunction<String, Tuple4<Integer, Integer, Integer, String>> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void flatMap(String tweetJsonStr, Collector<Tuple4<Integer, Integer, Integer, String>> collector) throws Exception {

            if (!tweetJsonStr.equals("")) {

                JsonNode tweetJson = mapper.readTree(tweetJsonStr);
                JsonNode text = tweetJson.get("text");
                if (text == null) return;

                JsonNode acc = tweetJson.get("user");
                if (acc == null) return;
                JsonNode name = acc.get("name");
                if (name == null) return;

                JsonNode accName = acc.get("screen_name");

                String twitterText = text.textValue();
                String extendedText = "";
                int truncated = 0;

                if (tweetJson.get("truncated").asBoolean()) {
                    truncated = 1;
                    extendedText = tweetJson.get("extended_tweet").get("full_text").textValue();
                } else {
                    if (tweetJson.has("retweeted_status")) {
                        if (tweetJson.get("retweeted_status").get("truncated").asBoolean()) {
                            truncated = 1;
                            extendedText = tweetJson.get("retweeted_status").get("extended_tweet").get("full_text").textValue();
                        }
                    }
                }

                boolean wordFilter = false;
                String[] splittedWords;
                if (truncated == 1) {
                    splittedWords = extendedText.toLowerCase().split("\\W+");
                } else {
                    splittedWords = twitterText.toLowerCase().split("\\W+");
                }
                for (String word : splittedWords) {
                    if (words.contains(word)
                    ) {
                        wordFilter = true;
                        break;
                    }
                }

                if (user.contains(accName.textValue())
                ) {
                    if (tweetJson.has("retweeted_status")) {
                        if (wordFilter) {

                            collector.collect(new Tuple4<>(1, 1, 1, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());

                        } else {
                            collector.collect(new Tuple4<>(1, 1, 0, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                        }
                    } else {
                        if (wordFilter) {
                            collector.collect(new Tuple4<>(1, 0, 1, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                        }else {
                            collector.collect(new Tuple4<>(1, 0, 0, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                        }
                    }


                }
            }
        }
    }

    private static class GetTopTweet implements AllWindowFunction<Tuple4<Integer, Integer, Integer, String>,
            Tuple5<Long, Integer, Integer, Integer, String>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple4<Integer, Integer, Integer, String>> tweets,
                          Collector<Tuple5<Long, Integer, Integer, Integer, String>> out) throws Exception {
            Tuple4<Integer, Integer, Integer, String> tweetOut = new Tuple4<>(0, 0, 0, "");

            for (Tuple4<Integer, Integer, Integer, String> tweet : tweets) {

                tweetOut.f0 = tweetOut.f0 + tweet.f0;
                tweetOut.f1 = tweetOut.f1 + tweet.f1;
                tweetOut.f2 = tweetOut.f2 + tweet.f2;
                tweetOut.f3 = tweet.f3;
            }
            out.collect(new Tuple5<>(window.getEnd(), tweetOut.f0, tweetOut.f1, tweetOut.f2, tweetOut.f3));
            System.out.println(tweetOut.f0 + " " + window.getEnd() + " " + tweetOut.f1 + " " + tweetOut.f2 + " " + tweetOut.f3
            );
        }
    }
}



