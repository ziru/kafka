### Create topics
```
kafka-topics --zookeeper localhost:2181 --create --topic favorite-colors-input --partitions 1 --replication-factor 1

kafka-topics --zookeeper localhost:2181 --create --topic favorite-colors-interim --config cleanup.policy=compact --partitions 1 --replication-factor 1

kafka-topics --zookeeper localhost:2181 --create --topic favorite-colors-output --partitions 1 --replication-factor 1
```

### Start the producer
```
kafkacat -P -b localhost:9092 -t favorite-colors-input
```

### Start the consumer
```
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic favorite-colors-output \
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
~
