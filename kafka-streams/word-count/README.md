### Start the producer
```
kafkacat -P -b localhost:9092 -t word-count-input
```

### Start the consumer
```
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic word-count-output \
  --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

### Compile and package the code
```
mvn package
```

### Start the Kafka streams app
```
java -jar target/wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar
```
