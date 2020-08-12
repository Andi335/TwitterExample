package examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
                "They call it a Royale with cheese.",
                "mama says, stupid is as stupid does",
                "good afternoon, good evening, and good night"
        );

        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new TextSplitter())
                        .groupBy(0)
                        .sum(1);

        counts.print();
    }


    private static class TextSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector){
            String[] words = s.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.length() > 0) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}


