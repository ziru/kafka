package com.github.ziru.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class FavoriteColorsApp {
    private static final Set<String> VALID_COLORS = new HashSet<>(Arrays.asList("green", "red", "blue"));;

    private static boolean isGoodValue(String value) {
        String[] tokens = value.split(",", -1);
        if (tokens.length != 2 || tokens[0].length() == 0) {
            return false;
        }
        return VALID_COLORS.contains(tokens[1]);
    }

    private static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // 1. read data from topic
        KStream<String, String> source = builder.stream("favorite-colors-input");

        source
                // 2. map values to lowercase
                .mapValues(v -> v.toLowerCase())
                // 3. filter bad values
                .filter((k, v) -> isGoodValue(v))
                // 3. extract key from values
                .map((k, v) -> KeyValue.pair(v.split(",")[0], v.split(",")[1]))
                // 4. send back to kafka
                .to("favorite-colors-interim", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> interim = builder
                // 5. read back as KTable
                .table("favorite-colors-interim");

        KTable<String, Long> favoriteColours = interim
                // 6. group by color within ktable
                .groupBy((user, color) -> KeyValue.pair(color, color))
                // 7. count
                .count();

        favoriteColours.toStream().to("favorite-colors-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    private static Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-favorite-colors");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return props;
    }

    public final static void main(String[] args) {
        Properties props = createProperties();
        Topology topology = createTopology();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println(topology.describe());
    }
}
