package standAlone;
import consoleProgramm.MainController;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class TrendSA {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        String propFileName = "config.properties";
        InputStream inputStream;
        inputStream = MainController.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            props.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        try {

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true).setOAuthConsumerKey(props.getProperty("twitter-source.consumerKey"))
                    .setOAuthConsumerSecret(props.getProperty("twitter-source.consumerSecret"))
                    .setOAuthAccessToken(props.getProperty("twitter-source.token"))
                    .setOAuthAccessTokenSecret(props.getProperty("twitter-source.tokenSecret"));

            TwitterFactory tf = new TwitterFactory(cb.build());
            Twitter twitter = tf.getInstance();

            Integer idTrendLocation = getTrendLocationId("germany", props); // adjust locationName

            if (idTrendLocation == null) {
                System.out.println("ConsoleProgramm.Trend Location Not Found");
                System.exit(0);
            }

            List<String> trendsList = new ArrayList<>();

            Trends trends = twitter.getPlaceTrends(idTrendLocation);
            for (int i = 0; i < trends.getTrends().length; i++) {
                trendsList.add(trends.getTrends()[i].getName());
            }

            System.out.println(trendsList);

            System.exit(0);

        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get trends: " + te.getMessage());
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Integer getTrendLocationId(String locationName, Properties props) {

        int idTrendLocation = 0;

        try {

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true).setOAuthConsumerKey(props.getProperty("twitter-source.consumerKey"))
                    .setOAuthConsumerSecret(props.getProperty("twitter-source.consumerSecret"))
                    .setOAuthAccessToken(props.getProperty("twitter-source.token"))
                    .setOAuthAccessTokenSecret(props.getProperty("twitter-source.tokenSecret"));

            TwitterFactory tf = new TwitterFactory(cb.build());
            Twitter twitter = tf.getInstance();

            ResponseList<Location> locations;
            locations = twitter.getAvailableTrends();

            for (Location location : locations) {
                if (location.getName().toLowerCase().equals(locationName.toLowerCase())) {
                    idTrendLocation = location.getWoeid();
                    break;
                }
            }

            if (idTrendLocation > 0) {
                return idTrendLocation;
            }

            return null;

        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get trends: " + te.getMessage());
            return null;
        }
    }
}