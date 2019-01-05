package com.github.ziru.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {
    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer(props);
    }
    
    public static void main(String[] args) {
        try (Producer<String, String> producer = createProducer()) {
            int i = 0;
            while (true) {
                System.out.println("Producing batch: " + i);
                try {
                    producer.send(newRandomTransaction("john"));
                    Thread.sleep(100);
                    producer.send(newRandomTransaction("stephane"));
                    Thread.sleep(100);
                    producer.send(newRandomTransaction("alice"));
                    Thread.sleep(100);
                    i += 1;
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    private static ProducerRecord<String, String> newRandomTransaction(String name) {
        ObjectNode transaction = OBJECT_MAPPER.createObjectNode();
        transaction.put("name", name);
        transaction.put("amount", ThreadLocalRandom.current().nextInt(0, 100));
        transaction.put("time", Instant.now().toString());
        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }
}
