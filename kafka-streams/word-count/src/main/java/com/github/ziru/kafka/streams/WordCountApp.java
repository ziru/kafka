package com.github.ziru.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    private static Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        // 1 - stream from Kafka
        final KStream<String, String> source = builder.stream("word-count-input");

        final KTable<String, Long> wordCounts = source
                // 2 - map values to lowercase
                .mapValues((v) -> v.toLowerCase())
                // 3 - flatmap values split by whitespace => zero to more items
                .flatMapValues((v) -> Arrays.asList(v.split("\\W+")))
                // 4 - select the word as the key
                .selectKey((k, v) -> v)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurrences
                .count();

        // 7 - output results back to Kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-count");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        Topology topology = createTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Print the topology for learning purposes
        System.out.println(topology.describe());
    }
}
