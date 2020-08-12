package examples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class TableExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<WordCount> input = env.fromElements(
                new WordCount("Cheeseburger", 1),
                new WordCount("Hamburger", 1),
                new WordCount("Cheeseburger", 1));

        tEnv.registerDataSet("WordCount", input, "word, frequency");
        Table table = tEnv.sql(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        DataSet<WordCount> result = tEnv.toDataSet(table, WordCount.class);
        result.print();
    }
    public static class WordCount {
        public String word;
        public long frequency;

        public WordCount(){
        }
        public WordCount(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }
        @Override
        public String toString() {
            return word + " " + frequency;
        }
    }
}



