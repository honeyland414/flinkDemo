package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkDemoApplication {
    public static void main( String[] args ) throws Exception {
        //batchWordCount();
        streamWordCount();
    }

    public static void streamWordCount() throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for(String word : words) {
                    Tuple2<String, Integer> tuple = Tuple2.of(word, 1);
                    out.collect(tuple);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> key = wordMap.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = key.sum(1);

        sum.print();
        env.execute();
    }

    //outdated
    public static void batchWordCount() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.readTextFile("input/word.txt");
        FlatMapOperator<String, Tuple2<String, Integer>> wordMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple = Tuple2.of(word, 1);
                    out.collect(wordTuple);
                }
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> wordGroupBy = wordMap.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> res = wordGroupBy.sum(1);

        res.print();

    }
}
