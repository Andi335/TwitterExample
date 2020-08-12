package consoleProgramm;

import consoleProgramm.twitterAnalysis.TermCount;
import consoleProgramm.twitterAnalysis.TopHashtag;
import consoleProgramm.twitterAnalysis.Trend;
import consoleProgramm.twitterAnalysis.TweetsCount;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;

public class MainController {
    static List<String> words = new ArrayList<>();
    static List<String> user = new ArrayList<>();
    static List<Long> userIds = new ArrayList<>();
    static String location;
    static int analysis = 0;
    static InputStream inputStream;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        String propFileName = "config.properties";
        inputStream = MainController.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            props.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        Scanner s = new Scanner(System.in);

        while (true) {
            words.clear();
            user.clear();
            userIds.clear();

            do {
                System.out.println("Welche Analyse soll durchgeführt werden?");
                System.out.println("1 für TweetsCount");
                System.out.println("2 für TopHashtag");
                System.out.println("3 für Trends");
                System.out.println("4 für TermsCounter");
                switch (s.next()) {
                    case "1":
                        analysis = 1;
                        break;
                    case "2":
                        analysis = 2;
                        break;
                    case "3":
                        analysis = 3;
                        break;
                    case "4":
                        analysis = 4;
                        break;
                    default:
                        System.out.println("Eingabe ungültig");
                }
            } while (analysis == 0);

            switch (analysis) {
                case 1:
                    filter();
                    userFollowing();
                    break;
                case 2:
                    userFollowing();
                    break;
                case 3:
                    System.out.println("Für welches Land sollen die Trends ausgegeben werden? ( z.B. germany, united_states)");
                    location = s.next();
                    break;
                case 4:
                    userFollowing();
                    break;
            }
            if (!(analysis == 3)) {
                System.out.println("Sind die Eingaben richtig? (j/n)");
                System.out.println("Begriffe: " + words);
                System.out.println("User: " + user);
                System.out.println("UserIds: " + userIds);
                String weiter = s.next();
                if (weiter.equals("j")) {
                    break;
                }
            }else{
                System.out.println("Sind die Eingaben richtig? (j/n)");
                System.out.println(location);
                String weiter = s.next();
                if (weiter.equals("j")) {
                    break;
                }
            }
        }
        switch (analysis) {
            case 1:
                System.out.println("TweetsCount wird ausgeführt");
                TweetsCount.main(props, user, words, userIds);
            case 2:
                System.out.println("TopHashtag wird ausgeführt");
                TopHashtag.main(props, user, userIds);
            case 3:
                System.out.println("Trends wird ausgeführt");
                Trend.main(props, location);
            case 4:
                System.out.println("TermsCounter wird ausgeführt");
                TermCount.main(props, user, userIds);
        }

    }

    public static void filter() {
        Scanner s = new Scanner(System.in);
        while (true) {
            System.out.println(words);
            System.out.println("Nach welchen Begriffen sollen die Tweets gefiltert werden? (\"q\" zum stoppen)");
            String s1 = s.next();
            if (s1.equals("q") && !words.isEmpty()) {
                break;
            }
            words.add(s1);
        }
    }

    public static void userFollowing() {
        int userCounter = 0;
        Scanner s = new Scanner(System.in);
        while (true) {
            System.out.println(user);
            System.out.println("Welche User sollen betrachtet werden?(Twittername ohne das \"@\" davor) (\"q\" zum stoppen)");
            String s2 = s.next();
            if (s2.equals("q") && !user.isEmpty()) {
                break;
            }
            userCounter = userCounter + 1;
            user.add(s2);
        }

        while (true) {
            System.out.println(userIds);
            System.out.println("Geben Sie die jeweiligen UserIds der Accounts ein (" + userCounter + " User wurden eingetragen)");

            String s3 = s.next();
            try {
                userIds.add(Long.valueOf(s3));
                if (userIds.size() == userCounter)
                    break;
            } catch (NumberFormatException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
