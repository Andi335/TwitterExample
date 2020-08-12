package consoleProgramm.twitterAnalysis;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class TermCount {
    static List<String> terms = new ArrayList<>();
    static List<String> user = new ArrayList<>();
    static List<Long> userIds = new ArrayList<>();

    public static void main(Properties props, List<String> scannedUser, List<Long> scannedUserIds) throws Exception {

        user = scannedUser;
        userIds = scannedUserIds;

        Scanner s = new Scanner(new File("german_non_nouns.txt"));
        while (s.hasNext()) {
            terms.add(s.next());
        }
        s.close();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TwitterSource twitterSource = new TwitterSource(props);
        TweetFilter customFilterInitializer = new TweetFilter();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);

        DataStream<String> streamSource = env.addSource(twitterSource);


        DataStream<Tuple5<String, Long, Integer, Integer, String>> source =

                streamSource.flatMap(new ExtractTwitterText())
                        .filter(new FilterTerms())
                        .keyBy(0)
                        .timeWindow(Time.seconds(1))
                        .sum(1)
                        .timeWindowAll(Time.seconds(1))
                        .apply(new GetTopTerm());


        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

        source.addSink(new ElasticsearchSink<>(
                config,
                transports,
                new TermInserter()));


        env.execute();
    }

    public static class TermInserter
            implements ElasticsearchSinkFunction<Tuple5<String, Long, Integer, Integer, String>> {

        // construct index request
        @Override
        public void process(
                Tuple5<String, Long, Integer, Integer, String> record,
                RuntimeContext ctx,
                RequestIndexer indexer) {

            // construct JSON document to index
            Map<String, String> json = new HashMap<>();
            json.put("term", record.f0);
            json.put("time", record.f1.toString()); // timestamp
            json.put("cnt", record.f2.toString());
            json.put("cntret", record.f3.toString());
            json.put("accName", record.f4);

            IndexRequest rqst = Requests.indexRequest()
                    .index("terms")        // index name
                    .type("topterms")  // mapping name
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

    private static class FilterTerms implements FilterFunction<Tuple4<String, Integer, Integer, String>> {
        @Override
        public boolean filter(Tuple4<String, Integer, Integer, String> terms) throws Exception {
            return !terms.f0.equals("-") &&
                    !terms.f0.equals("–") && !terms.f0.equals("co") &&
                    !terms.f0.equals("https") && !terms.f0.matches("[0-9]+");
        }
    }


    private static class ExtractTwitterText implements FlatMapFunction<String, Tuple4<String, Integer, Integer, String>> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void flatMap(String tweetJsonStr, Collector<Tuple4<String, Integer, Integer, String>> collector) throws Exception {

            if (!tweetJsonStr.equals("")) {

                JsonNode tweetJson = mapper.readTree(tweetJsonStr);
                JsonNode text = tweetJson.get("text");
                if (text == null) return;

                JsonNode acc = tweetJson.get("user");
                if (acc == null) return;
                JsonNode accName = acc.get("screen_name");

                String twitterText = text.textValue();
                String extendedText = "";

                List<String> urlss = new ArrayList<>();

                int truncated = 0;

                if (tweetJson.get("truncated").asBoolean()) {
                    truncated = 1;
                    extendedText = tweetJson.get("extended_tweet").get("full_text").textValue();

                    for (Iterator<JsonNode> iter = tweetJson.get("extended_tweet").get("entities").get("urls").elements(); iter.hasNext(); ) {
                        JsonNode node = iter.next();
                        String[] s = node.get("url").textValue().split("https://t.co/");
                        urlss.addAll(Arrays.asList(s));
                    }
                    if (tweetJson.get("extended_tweet").get("entities").has("media")) {
                        for (Iterator<JsonNode> iter = tweetJson.get("extended_tweet").get("entities").get("media").elements(); iter.hasNext(); ) {
                            JsonNode node = iter.next();
                            String[] s = node.get("url").textValue().split("https://t.co/");
                            urlss.addAll(Arrays.asList(s));
                        }
                    }

                } else {
                    if (tweetJson.has("retweeted_status")) {
                        if (tweetJson.get("retweeted_status").get("truncated").asBoolean()) {
                            truncated = 1;
                            extendedText = tweetJson.get("retweeted_status").get("extended_tweet").get("full_text").textValue();
                            for (Iterator<JsonNode> iter = tweetJson.get("retweeted_status").get("extended_tweet").get("entities").get("urls").elements(); iter.hasNext(); ) {
                                JsonNode node = iter.next();
                                String[] s = node.get("url").textValue().split("https://t.co/");
                                urlss.addAll(Arrays.asList(s));

                            }
                            if (tweetJson.get("retweeted_status").get("extended_tweet").get("entities").has("media")) {
                                for (Iterator<JsonNode> iter = tweetJson.get("retweeted_status").get("extended_tweet").get("entities").get("media").elements(); iter.hasNext(); ) {
                                    JsonNode node = iter.next();
                                    String[] s = node.get("url").textValue().split("https://t.co/");
                                    urlss.addAll(Arrays.asList(s));
                                }
                            }
                        } else {
                            twitterText = tweetJson.get("retweeted_status").get("text").textValue();
                            for (Iterator<JsonNode> iter = tweetJson.get("retweeted_status").get("entities").get("urls").elements(); iter.hasNext(); ) {
                                JsonNode node = iter.next();
                                String[] s = node.get("url").textValue().split("https://t.co/");
                                urlss.addAll(Arrays.asList(s));
                            }
                            if (tweetJson.get("retweeted_status").get("entities").has("media")) {
                                for (Iterator<JsonNode> iter = tweetJson.get("retweeted_status").get("entities").get("media").elements(); iter.hasNext(); ) {
                                    JsonNode node = iter.next();
                                    String[] s = node.get("url").textValue().split("https://t.co/");
                                    urlss.addAll(Arrays.asList(s));
                                }
                            }
                        }
                    } else {
                        for (Iterator<JsonNode> iter = tweetJson.get("entities").get("urls").elements(); iter.hasNext(); ) {
                            JsonNode node = iter.next();
                            String[] s = node.get("url").textValue().split("https://t.co/");
                            urlss.addAll(Arrays.asList(s));
                        }
                        if (tweetJson.get("entities").has("media")) {
                            for (Iterator<JsonNode> iter = tweetJson.get("entities").get("media").elements(); iter.hasNext(); ) {
                                JsonNode node = iter.next();
                                String[] s = node.get("url").textValue().split("https://t.co/");
                                urlss.addAll(Arrays.asList(s));
                            }
                        }
                    }
                }


                if (user.contains(accName.textValue())
                ) {
                    System.out.println(tweetJsonStr);
                    if (truncated == 1) {
                        for (String word : extendedText.split(" (?u)|[.!?/,\"'()`:´’„“;><={}&%$§€+*|]|&amp")) {
                            if (!word.isEmpty() && !terms.contains(word.toLowerCase()) && !urlss.contains(word)) {
                                if (tweetJson.has("retweeted_status")) {
                                    collector.collect(new Tuple4<>(word, 1, 1, accName.textValue()));
                                } else {
                                    collector.collect(new Tuple4<>(word, 1, 0, accName.textValue()));
                                }
                            }
                        }
                    } else {
                        for (String word : twitterText.split(" (?u)|[.!?/,\"'()`:´’„“;><={}&%$§€+*|]|&amp")) {
                            if (!word.isEmpty() && !terms.contains(word.toLowerCase())&& !urlss.contains(word)) {
                                if (tweetJson.has("retweeted_status")) {
                                    collector.collect(new Tuple4<>(word, 1, 1, accName.textValue()));
                                } else {
                                    collector.collect(new Tuple4<>(word, 1, 0, accName.textValue()));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static class GetTopTerm implements AllWindowFunction<Tuple4<String, Integer, Integer, String>, Tuple5<String, Long, Integer, Integer, String>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple4<String, Integer, Integer, String>> tweets, Collector<Tuple5<String, Long, Integer, Integer, String>> out) throws Exception {
            for (Tuple4<String, Integer, Integer, String> tweet : tweets) {
                out.collect(new Tuple5<>(tweet.f0, window.getEnd(), tweet.f1, tweet.f2, tweet.f3));
                System.out.println(tweet.f0 + " " + window.getEnd() + " " + tweet.f1 + " " + tweet.f2 + " " + tweet.f3);

            }
        }
    }
}


