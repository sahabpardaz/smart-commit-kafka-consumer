package ir.sahab.kafkaconsumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SmartCommitKafkaConsumerTest {

    private static final String TOPIC_NAME = "test";
    private static final Integer NUM_PARTITIONS = 3;
    private static EmbeddedKafkaServer kafkaServer;

    @BeforeClass
    public static void setupClass() throws IOException, InterruptedException {
        kafkaServer = new EmbeddedKafkaServer();
        kafkaServer.start();
        kafkaServer.createTopic(TOPIC_NAME, NUM_PARTITIONS);
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        kafkaServer.close();
    }

    /**
     * In this test, we are going to produce several records and concurrently read them by several
     * {@link SmartCommitKafkaConsumer} instances. The first consumer instance does not read all of
     * the records, and stops after reading some. Then we expect that a second consumer which
     * becomes live on a same consumer group, consumed all records which are not acked yet
     * (i.e., the ones which are not committed according to the offset tracker logic of the
     * transporter source).
     */
    @Test
    public void test() throws Exception {
        // Start Kafka producer thread: send several records to the topic.
        int numRecordsToProduce = 1000;
        String kafkaServers = kafkaServer.getBrokerAddress();
        Thread producerThread = new Thread(() -> {
            Map<String, Object> config = new HashMap<>();
            config.put(BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
            config.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            config.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            try (KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(config)) {
                for (int i = 0; i < numRecordsToProduce; i++) {
                    kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, new byte[100]));
                }
                kafkaProducer.flush();
            }

        });
        producerThread.start();

        // Start first consumer: read/ack several records (not all of them).
        Map<Integer /*partition*/, Long /*offset*/> maxAckedByFirstConsumer = new HashMap<>();
        Properties consumerProperties = new Properties();
        String groupId = "custom-group";
        consumerProperties.put(GROUP_ID_CONFIG, groupId);
        consumerProperties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getBrokerAddress());
        consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        int pageSize = 10;
        int maxOpenPages = 1000;
        int numRecordsToAck = 763;

        try (SmartCommitKafkaConsumer<byte[], byte[]> kafkaConsumer =
                new SmartCommitKafkaConsumer<>(consumerProperties, pageSize, maxOpenPages,
                        2 * numRecordsToProduce)) {

            kafkaConsumer.subscribe(TOPIC_NAME);
            kafkaConsumer.start();

            List<PartitionOffset> partitionOffsets = new ArrayList<>(numRecordsToAck);
            while (partitionOffsets.size() < numRecordsToAck) {
                ConsumerRecord<byte[], byte[]> record = kafkaConsumer.poll();
                if (record == null) {
                    Thread.sleep(1);
                    continue;
                }
                partitionOffsets.add(new PartitionOffset(record.partition(), record.offset()));
                maxAckedByFirstConsumer.put(record.partition(), record.offset());
            }

            // Acks are not necessary ordered so we shuffle them.
            Collections.shuffle(partitionOffsets);
            for (PartitionOffset partitionOffset : partitionOffsets) {
                kafkaConsumer.ack(partitionOffset);
            }

            // Sleep to make sure, the async. commits of transporter source is finished.
            Thread.sleep(1000);
        }

        // Start another Kafka consumer to read the remaining records.
        Map<Integer /*partition*/, Long /*offset*/> minPolledBySecondConsumer = new HashMap<>();
        Map<Integer /*partition*/, Long /*offset*/> maxPolledBySecondConsumer = new HashMap<>();
        Properties props = new Properties();
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getBrokerAddress());
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<byte[], byte[]> secondConsumer = new KafkaConsumer<>(props)) {
            secondConsumer.subscribe(Collections.singleton(TOPIC_NAME));

            ConsumerRecords<byte[], byte[]> records;
            do {
                records = secondConsumer.poll(10000);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    minPolledBySecondConsumer.merge(record.partition(), record.offset(), Math::min);
                    maxPolledBySecondConsumer.merge(record.partition(), record.offset(), Math::max);
                }
            } while (!records.isEmpty());
        }

        // This consumer should receive the remaining records (from the last offset committed by
        // SmartCommitKafkaConsumer object).
        Assert.assertFalse(minPolledBySecondConsumer.isEmpty());
        long numRecordsReadTotally = 0;
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            // Commit offsets are done on page heads. (See OffsetTracker for more details)
            long expectedInitialOffsetToPoll =
                    (maxAckedByFirstConsumer.getOrDefault(i, 0L) + 1) / pageSize * pageSize;
            Long minOffsetPolled = minPolledBySecondConsumer.get(i);

            if (minOffsetPolled != null) {
                assertEquals(expectedInitialOffsetToPoll, minOffsetPolled.longValue());
                numRecordsReadTotally += maxPolledBySecondConsumer.get(i) + 1;
            } else {
                numRecordsReadTotally += expectedInitialOffsetToPoll + 1;
            }
        }
        assertEquals(numRecordsToProduce, numRecordsReadTotally);
    }
}