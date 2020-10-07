# Smart Commit Kafka Consumer
It is a wrapper around Kafka consumer which implements *smart commit* feature.
The client neither commits the offsets manually nor uses the default *auto commit* feature,
but acks the records which their process is completed.
Then the offsets will be committed automatically in a manner which *at least once delivery*
is guaranteed. 
So it keeps track of all offsets until receiving theirs acks  but it is hidden
from the client. In fact, offsets of each partition are tracked in several
pages, each of them responsible for a specific range of offsets.
When all offsets of some consecutive pages are acked, the last offset of the last fully acked page will be committed automatically.

## Sample Usage
```java
try (SmartCommitKafkaConsumer kafkaConsumer = new SmartCommitKafkaConsumer(consumerProperties)) {
        kafkaConsumer.subscribe(topic);  // You can assign(topic, partitions) instead.
  
        while (shouldContinue()) {
            executorService.submit(() -> {
                ConsumerRecord record = kafkaConsumer.poll();
                process(record);
                kafkaConsumer.ack(new PartitionOffset(record.partition(), record.offset()));
            });
        }
}
```
## Add it to your project
You can reference to this library by either of java build systems (Maven, Gradle, SBT or Leiningen) using snippets from this jitpack link:
[![](https://jitpack.io/v/sahabpardaz/smart-commit-kafka-consumer.svg)](https://jitpack.io/#sahabpardaz/smart-commit-kafka-consumer)
