package standAlone;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import consoleProgramm.MainController;
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

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class TweetsCountSA {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        String propFileName = "config.properties";
        InputStream inputStream;
        inputStream = MainController.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            props.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        TwitterSource twitterSource = new TwitterSource(props);
        TweetFilter customFilterInitializer = new TweetFilter();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);

        DataStream<String> streamSource = env.addSource(twitterSource);


        DataStream<Tuple9<Long, Integer, Integer, Integer, String, Integer, Integer, Integer, String>> source =

                streamSource.flatMap(new ExtractTwitterText())
                        .keyBy(0)
                        .timeWindow(Time.milliseconds(500))
                        .sum(1)
                        .timeWindowAll(Time.seconds(1))
                        .apply(new GetTopTweet());
        source.print();
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
            implements ElasticsearchSinkFunction<Tuple9<Long, Integer, Integer, Integer, String, Integer, Integer, Integer, String>> {

        // construct index request
        @Override
        public void process(
                Tuple9<Long, Integer, Integer, Integer, String, Integer, Integer, Integer, String> record,
                RuntimeContext ctx,
                RequestIndexer indexer) {

            // construct JSON document to index
            Map<String, String> json = new HashMap<>();
            json.put("time", record.f0.toString());// timestamp
            json.put("cnttweet", record.f1.toString());
            json.put("cntret", record.f2.toString());
            json.put("cntword", record.f3.toString());
            json.put("accName", record.f4);
            json.put("cnttweet1", record.f5.toString());
            json.put("cntret1", record.f6.toString());
            json.put("cntword1", record.f7.toString());
            json.put("accName1", record.f8);
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
            //BILD, derspiegel, focusonline, ntvde, tagesschau, faznet, zeitonline, welt, reuters_de, heutejournal, dw_deutsch, fr
            //cnnbrk, Suntimes, NBCNews, nytimes, USATODAY, CBSNews, ABC, OANN, latimes, washingtonpost, TheOnion, TIME
            endpoint.followings(Arrays.asList(9204502L, 2834511L, 5494392L, 19232587L, 5734902L, 18047862L, 5715752L, 8720562L, 55035997L, 2527977343L, 6027992L, 17835938L,
                    428333L, 12811952L, 14173315L, 807095L, 15754281L, 15012486L, 28785486L, 1209936918L, 16664681L, 2467791L, 14075928L, 14293310L)); // american
            return endpoint;
        }
    }

    private static class ExtractTwitterText implements FlatMapFunction<String, Tuple8<Integer, Integer, Integer, String, Integer, Integer, Integer, String>> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void flatMap(String tweetJsonStr, Collector<Tuple8<Integer, Integer, Integer, String, Integer, Integer, Integer, String>> collector) throws Exception {

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

                boolean trump = false;
                String[] splittedWords;
                if (truncated == 1) {
                    splittedWords = extendedText.toLowerCase().split("\\W+");
                } else {
                    splittedWords = twitterText.toLowerCase().split("\\W+");
                }
                for (String word : splittedWords) {
                    if (word.equals("trump") ||
                            word.equals("donaldtrump") ||
                            word.equals("realdonaldtrump")
                    ) {
                        trump = true;
                        break;
                    }
                }

                if (accName.textValue().equals("BILD") |
                        accName.textValue().equals("derspiegel") |
                        accName.textValue().equals("focusonline") |
                        accName.textValue().equals("ntvde") |
                        accName.textValue().equals("tagesschau") |
                        accName.textValue().equals("faznet") |
                        accName.textValue().equals("zeitonline") |
                        accName.textValue().equals("welt") |
                        accName.textValue().equals("reuters_de") |
                        accName.textValue().equals("dw_deutsch") |
                        accName.textValue().equals("fr") |
                        accName.textValue().equals("heutejournal")
                ) {
                    if (tweetJson.has("retweeted_status")) {
                        if (trump) {
                            //(tweets,reetweet,trump,name, ... dito amerika)

                            collector.collect(new Tuple8<>(1, 1, 1, name.textValue(), 0, 0, 0, ""));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                            System.out.println(tweetJsonStr);

                        } else {
                            collector.collect(new Tuple8<>(1, 1, 0, name.textValue(), 0, 0, 0, ""));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                            System.out.println(tweetJsonStr);
                        }
                    } else {
                        if (trump) {
                            collector.collect(new Tuple8<>(1, 0, 1, name.textValue(), 0, 0, 0, ""));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                            System.out.println(tweetJsonStr);

                        }
                        collector.collect(new Tuple8<>(1, 0, 0, name.textValue(), 0, 0, 0, ""));
                        System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                        System.out.println(tweetJsonStr);
                    }
                }

                if (accName.textValue().equals("cnnbrk") |
                        accName.textValue().equals("Suntimes") |
                        accName.textValue().equals("NBCNews") |
                        accName.textValue().equals("nytimes") |
                        accName.textValue().equals("USATODAY") |
                        accName.textValue().equals("CBSNews") |
                        accName.textValue().equals("ABC") |
                        accName.textValue().equals("OANN") |
                        accName.textValue().equals("latimes") |
                        accName.textValue().equals("TheOnion") |
                        accName.textValue().equals("TIME") |
                        accName.textValue().equals("washingtonpost")
                ) {
                    if (tweetJson.has("retweeted_status")) {
                        if (trump) {
                            //(tweets,retweet,trump,name, ... dito amerika)
                            collector.collect(new Tuple8<>(0, 0, 0, "", 1, 1, 1, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                            System.out.println(tweetJsonStr);
                        } else {
                            collector.collect(new Tuple8<>(0, 0, 0, "", 1, 1, 0, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                            System.out.println(tweetJsonStr);
                        }
                    } else {
                        if (trump) {
                            collector.collect(new Tuple8<>(0, 0, 0, "", 1, 0, 1, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                            System.out.println(tweetJsonStr);

                        } else {
                            collector.collect(new Tuple8<>(0, 0, 0, "", 1, 0, 0, name.textValue()));
                            System.out.println(accName.textValue() + " " + tweetJson.get("created_at").textValue());
                            System.out.println(tweetJsonStr);
                        }
                    }
                }
            }
        }
    }

    private static class GetTopTweet implements AllWindowFunction<Tuple8<Integer, Integer, Integer, String, Integer, Integer, Integer, String>,
            Tuple9<Long, Integer, Integer, Integer, String, Integer, Integer, Integer, String>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, String, Integer, Integer, Integer, String>> tweets,
                          Collector<Tuple9<Long, Integer, Integer, Integer, String, Integer, Integer, Integer, String>> out) throws Exception {
            Tuple8<Integer, Integer, Integer, String, Integer, Integer, Integer, String> tweetOut = new Tuple8<>(0, 0, 0, "", 0, 0, 0, "");

            for (Tuple8<Integer, Integer, Integer, String, Integer, Integer, Integer, String> tweet : tweets) {

                tweetOut.f0 = tweetOut.f0 + tweet.f0;
                tweetOut.f1 = tweetOut.f1 + tweet.f1;
                tweetOut.f2 = tweetOut.f2 + tweet.f2;
                tweetOut.f3 = tweet.f3;
                tweetOut.f4 = tweetOut.f4 + tweet.f4;
                tweetOut.f5 = tweetOut.f5 + tweet.f5;
                tweetOut.f6 = tweetOut.f6 + tweet.f6;
                tweetOut.f7 = tweet.f7;
            }
            // tweets,ret,trump,name
            out.collect(new Tuple9<>(window.getEnd(), tweetOut.f0, tweetOut.f1, tweetOut.f2, tweetOut.f3, tweetOut.f4, tweetOut.f5, tweetOut.f6, tweetOut.f7));
            System.out.println(tweetOut.f0 + " " + window.getEnd() + " " + tweetOut.f1 + " " + tweetOut.f2 + " " + tweetOut.f3 + " " +
                    "amer.: " + tweetOut.f4 + " " + tweetOut.f5 + " " + tweetOut.f6 + " " + tweetOut.f7);
        }
    }
}



