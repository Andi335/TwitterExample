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

public class TopHashtagSA {
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
        DataStream<String> source = env.addSource(twitterSource);


        DataStream<Tuple9<String, Integer, Integer, String, String, Integer, Integer, String, Long>> streamsource =

                source.flatMap(new ExtractTwitterText())
                        .keyBy(0)
                        .timeWindow(Time.seconds(1))
                        .sum(1)
                        .timeWindowAll(Time.seconds(1))
                        .apply(new GetTopHashtag());


        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

        streamsource.addSink(new ElasticsearchSink<>(
                config,
                transports,
                new PopularHashtags()));


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
            json.put("accName1", record.f7);
            json.put("cnt1", record.f5.toString());
            json.put("cntret1", record.f6.toString());
            json.put("hashtag1", record.f4);

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

            // BILD, derspiegel, focusonline, ntvde, tagesschau, faznet, zeitonline, welt, reuters_de, heutejournal, dw_deutsch, fr
            //cnnbrk, Suntimes, NBCNews, nytimes, USATODAY, CBSNews, ABC, OANN, latimes, washingtonpost, TheOnion, TIME

            endpoint.followings(Arrays.asList(9204502L, 2834511L, 5494392L, 19232587L, 5734902L, 18047862L, 5715752L, 8720562L, 55035997L, 2527977343L, 6027992L, 17835938L,
                    428333L, 12811952L, 14173315L, 807095L, 15754281L, 15012486L, 28785486L, 1209936918L, 16664681L, 2467791L, 14075928L, 14293310L));
            return endpoint;
        }
    }

    private static class ExtractTwitterText implements FlatMapFunction<String, Tuple8<String, Integer, Integer, String, String, Integer, Integer, String>> {

        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void flatMap(String tweetJsonStr, Collector<Tuple8<String, Integer, Integer, String, String, Integer, Integer, String>> collector) throws Exception {
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

                if (accName.textValue().equals("BILD") |
                        accName.textValue().equals("derspiegel") |
                        accName.textValue().equals("ntvde") |
                        accName.textValue().equals("focusonline") |
                        accName.textValue().equals("tagesschau") |
                        accName.textValue().equals("faznet") |
                        accName.textValue().equals("zeitonline") |
                        accName.textValue().equals("welt") |
                        accName.textValue().equals("reuters_de") |
                        accName.textValue().equals("dw_deutsch") |
                        accName.textValue().equals("fr") |
                        accName.textValue().equals("heutejournal")
                ) {

                    for (Iterator<JsonNode> iter = hashtags.elements(); iter.hasNext(); ) {
                        JsonNode node = iter.next();
                        String hashtag = node.get("text").textValue();
                        if (!hashtag.isEmpty() && tweetJson.has("retweeted_status")) {
                            collector.collect(new Tuple8<>(hashtag, 1, 1, name.textValue(), "", 0, 0, ""));
                            System.out.println(tweetJsonStr);
                        }
                        if (!hashtag.isEmpty() && !tweetJson.has("retweeted_status")) {
                            collector.collect(new Tuple8<>(hashtag, 1, 0, name.textValue(), "", 0, 0, ""));
                            System.out.println(tweetJsonStr);
                        }
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
                    for (Iterator<JsonNode> iter = hashtags.elements(); iter.hasNext(); ) {
                        JsonNode node = iter.next();
                        String hashtag = node.get("text").textValue();
                        if (!hashtag.isEmpty() && tweetJson.has("retweeted_status")) {
                            //hash,cnt,reet,name...
                            collector.collect(new Tuple8<>("", 0, 0, "", hashtag, 1, 1, name.textValue()));
                            System.out.println(tweetJsonStr);
                        }
                        if (!hashtag.isEmpty() && !tweetJson.has("retweeted_status")) {
                            collector.collect(new Tuple8<>("", 0, 0, "", hashtag, 1, 0, name.textValue()));
                            System.out.println(tweetJsonStr);
                        }
                    }
                }

            }
        }
    }

    private static class GetTopHashtag implements AllWindowFunction<Tuple8<String, Integer, Integer, String, String, Integer, Integer, String>,
            Tuple9<String, Integer, Integer, String, String, Integer, Integer, String, Long>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple8<String, Integer, Integer, String, String, Integer, Integer, String>> hashtags,
                          Collector<Tuple9<String, Integer, Integer, String, String, Integer, Integer, String, Long>> out) throws Exception {

            for (Tuple8<String, Integer, Integer, String, String, Integer, Integer, String> hashTag : hashtags) {

                out.collect(new Tuple9<>(hashTag.f0, hashTag.f1, hashTag.f2, hashTag.f3, hashTag.f4, hashTag.f5, hashTag.f6, hashTag.f7, window.getEnd()));
                System.out.println(hashTag.f0 + hashTag.f1 + hashTag.f2 + hashTag.f3 + hashTag.f4 + hashTag.f5 + hashTag.f6 + hashTag.f7 + window.getEnd());

            }
        }
    }
}


