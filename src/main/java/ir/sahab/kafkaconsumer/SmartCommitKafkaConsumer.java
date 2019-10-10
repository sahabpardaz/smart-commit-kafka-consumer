package ir.sahab.kafkaconsumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import ir.sahab.logthrottle.LogThrottle;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A wrapper on {@link KafkaConsumer} which implements <i>smart commit</i> feature.
 * The client neither commits the offsets manually nor uses the default <i>auto commit</i> feature.
 * Instead calls {@link #ack(PartitionOffset)} for the records which their process is completed,
 * then the offsets will be committed automatically in a manner which <i>at least once delivery</i>
 * is guaranteed. For this reason an {@link OffsetTracker} object is used but it is hidden
 * from the client of this class. In fact, offsets of each partition are tracked in several
 * pages, each of them responsible for a specific range of offsets.
 * When all offsets of some consecutive pages are acked, the last offset will be committed
 * automatically.
 *
 * Here is its sample usage:
 * <pre>
 *  try (SmartCommitKafkaConsumer kafkaConsumer = new SmartCommitKafkaConsumer(consumerProperties)) {
 *      kafkaConsumer.subscribe(topic);  // You can assign(topic, partitions) instead.
 *
 *      while (shouldContinue()) {
 *          executorService.submit(() -> {
 *              ConsumerRecord record = kafkaConsumer.poll();
 *              process(record);
 *              try {
 *                  kafkaConsumer.ack(new PartitionOffset(record.partition(), record.offset()));
 *              } catch (InterruptedException e) {
 *                  return;  // We are interrupted as shut down process. So better to give it up.
 *              }
 *          });
 *      }
 *  }
 *
 * Note that offsets will be committed in pages
 * </pre>
 */
public class SmartCommitKafkaConsumer<K, V> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SmartCommitKafkaConsumer.class);
    private static final LogThrottle logThrottle = new LogThrottle(logger);

    private static final int POLL_TIMEOUT_MILLIS = 10;

    /**
     * This is the maximum expected value of the processing throughput. It is also bound
     * to the Kafka consumption rate. We have used a value which is big enough that is not
     * reachable in almost all use cases (1 million per second).
     */
    private static final int MAX_EXPECTED_INPUT_ACKS_PER_MILLIS = 1000;

    /**
     * We do not want to block caller on {@link #ack(PartitionOffset)}. So for the queue size of
     * the {@link #unappliedAcks}  we have calculated the maximum number of acks which can become
     * pending before returning from {@link KafkaConsumer#poll(long)} method.
     */
    private static final int MAX_UNAPPLIED_ACKS =
            MAX_EXPECTED_INPUT_ACKS_PER_MILLIS * POLL_TIMEOUT_MILLIS;

    /**
     * The underlying Kafka consumer which is wrapped by this class.
     */
    private final KafkaConsumer<K, V> kafkaConsumer;

    /**
     * Despite original Kafka consumer, this consumer is active. By calling {@link #start()}, it
     * starts polling the records from its internal thread.
     */
    private final Thread thread;

    /**
     * It provides the offsets which are safe to commit by tracking the offsets which are polled
     * and the offsets which are processed and their acks are received.
     */
    private final OffsetTracker offsetTracker;

    /**
     * The queued records before delivering to the client, are kept here.
     */
    private final BlockingQueue<ConsumerRecord<K, V>> queuedRecords;

    /**
     * The acks which are not yet applied to the {@link #offsetTracker} are kept in this queue.
     */
    private final BlockingQueue<PartitionOffset> unappliedAcks;

    /**
     * The callback to set on calling {@link KafkaConsumer#commitAsync(OffsetCommitCallback)}.
     * We have used a same callback that does nothing other than writing simple logs.
     */
    private final OffsetCommitCallback offsetCommitCallback;

    /**
     * The callback to set on calling {@link KafkaConsumer#subscribe(Collection)}.
     * We have used a same callback that does nothing other than writing simple logs and resetting
     * the {@link #offsetTracker}.
     */
    private final ConsumerRebalanceListener rebalanceListener;

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private JmxReporter reporter;

    private String topic;
    private volatile boolean stop = false;

    /**
     * Constructs a smart Kafka consumer using default values for page size, max open pages and
     * max queued records.
     * @param kafkaConsumerProperties the properties of {@link KafkaConsumer}. It should contains
     * at least bootstrap servers, serializer and de-serializer classes. Because of the smart
     * commit feature, the 'enable_auto_commit_config' should not be activated.
     * @throws IllegalArgumentException if the mandatory Kafka consumer properties are not provided,
     * or other argument values are not in their expected valid ranges.
     */
    public SmartCommitKafkaConsumer(Properties kafkaConsumerProperties) {
        this(kafkaConsumerProperties, 10_000, 1000, 10_000);
    }

    /**
     * @param kafkaConsumerProperties the properties of {@link KafkaConsumer}. It should contains
     * at least bootstrap servers, serializer and de-serializer classes. Because of the smart
     * commit feature is not consistent with auto commit, the config 'enable_auto_commit_config'
     * will be override with value 'false'.
     * @param offsetTrackerPageSize the size of each page in offset tracker. Offsets will be
     * committed just when some consecutive pages become fully acked. In fact lower page sizes,
     * causes more frequent commits.
     * @param offsetTrackerMaxOpenPagesPerPartition maximum number of open pages (pages which have
     * tracked but not acked offsets). After reaching to this limit on a partition, reading from
     * Kafka topic will be blocked, waiting for receiving more pending acks from the client.
     * A good choice is to completely avoid this kind of blockage. For this reason, it is
     * sufficient to satisfy this equation:
     * <pre> (pageSize * maxOpenPages * numPartitions) > (maximum number of pending records) </pre>
     * In the above equation, by pending records we mean the ones which are polled but not yet
     * acked.
     * @param maxQueuedRecords maximum number of records which can be queued to be later polled by
     * the client.
     * @throws IllegalArgumentException if the mandatory Kafka consumer properties are not provided,
     * or other argument values are not in their expected valid ranges.
     */
    public SmartCommitKafkaConsumer(Properties kafkaConsumerProperties,
            int offsetTrackerPageSize, int offsetTrackerMaxOpenPagesPerPartition,
            int maxQueuedRecords) {

        requireNonNull(kafkaConsumerProperties);
        checkArgument(kafkaConsumerProperties.containsKey(BOOTSTRAP_SERVERS_CONFIG));
        checkArgument(kafkaConsumerProperties.containsKey(KEY_DESERIALIZER_CLASS_CONFIG));
        checkArgument(kafkaConsumerProperties.containsKey(VALUE_DESERIALIZER_CLASS_CONFIG));
        checkArgument(offsetTrackerPageSize > 0);
        checkArgument(offsetTrackerMaxOpenPagesPerPartition > 0);
        checkArgument(maxQueuedRecords > 0);

        // Init objects regarding to consuming from Kafka.
        kafkaConsumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
        this.queuedRecords = new ArrayBlockingQueue<>(maxQueuedRecords);
        this.thread = initConsumerThread();

        // Init objects regarding to offset track.
        this.offsetTracker = new OffsetTracker(
                offsetTrackerPageSize, offsetTrackerMaxOpenPagesPerPartition);
        offsetCommitCallback = (offsets, e) -> {
            if (e != null) {
                logThrottle.logger("commit-failed").warn(
                        "Failed to commit offset. It is valid just if Kafka is out of reach "
                        + "or it was in a re-balance process recently.", e);
            } else {
                logger.debug("Offsets committed: " + offsets);
            }
        };
        this.rebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    logger.warn("Kafka consumer previous assignment revoked: {}", partitions);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("Kafka consumer partitions assigned: {}", partitions);
                offsetTracker.reset();
            }
        };
        this.unappliedAcks = new ArrayBlockingQueue<>(MAX_UNAPPLIED_ACKS);
    }

    /**
     * Subscribes to the given topic by this consumer. Partitions will be assigned dynamically.
     * Either this method or {@link #assign(String, List)} should be
     * called just once after construction and before calling {@link #start()}.
     */
    public void subscribe(String topic) {
        checkState(this.topic == null, "It is already subscribed/assigned. You should call either "
                + "subscribe() or assign() just once.");
        checkArgument(topic != null && !topic.isEmpty());

        kafkaConsumer.subscribe(Collections.singleton(topic), rebalanceListener);
        this.topic = topic;
    }

    /**
     * Manually assigns the given partition form the requested topic to this consumer.
     * Either this method or {@link #subscribe(String)} should be
     * called just once after construction and before calling {@link #start()}.
     */
    public void assign(String topic, List<Integer> partitions) {
        checkState(this.topic == null, "It is already subscribed/assigned. You should call either "
                + "subscribe() or assign() just once.");
        checkArgument(topic != null && !topic.isEmpty());
        requireNonNull(partitions);
        checkArgument(!partitions.isEmpty());

        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitions.forEach(p -> topicPartitions.add(new TopicPartition(topic, p)));
        kafkaConsumer.assign(topicPartitions);
        this.topic = topic;
    }

    /**
     * First ensures connectivity to the Kafka topic and then starts polling from the Kafka topic
     * in a background thread. Then the polled records can be accessed via {@link #poll()}.
     * @throws IOException if can not establish connection to the Kafka topic.
     */
    public void start() throws InterruptedException, IOException {
        checkState(topic != null, "You should first call either subscribe() or assign().");
        checkState(!thread.isAlive(), "start() is called but Kafka consumer is already started.");
        logger.info("Starting smart commit kafka consumer of {} topic...", topic);
        initMetrics();

        // Ensure connection to Kafka topic.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> kafkaConsumer.poll(0)).get(60, SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Failed connecting to Kafka.", e);
        } finally {
            executor.shutdown();
        }

        thread.start();
    }

    /**
     * Registers metrics and starts JMX reporter.
     */
    private void initMetrics() {
        metricRegistry.register("unappliedAcks", (Gauge) unappliedAcks::size);
        metricRegistry.register("queuedRecordsSize", (Gauge) queuedRecords::size);
        reporter = JmxReporter.forRegistry(metricRegistry)
                              .inDomain("smart-commit-kafka-consumer." + topic)
                              .build();
        reporter.start();
    }

    /**
     * Retrieves and removes the head of the queued consumer records.
     * @return head of the queue, or {@code null} if there is no item left.
     */
    public ConsumerRecord<K, V> poll() {
        return queuedRecords.poll();
    }

    /**
     * Retrieves and removes at most the given number of available elements from queue.
     * @return the number of elements transferred.
     */
    public int drainTo(Collection<? super ConsumerRecord<K, V>> collection, int maxElements) {
        return queuedRecords.drainTo(collection, maxElements);
    }


    /**
     * Informs that the given offset is processed. When the acks fills some consecutive pages of a
     * partition, the last offset of those completed pages will be committed.
     * @throws InterruptedException if interrupted while waiting for a free room in the queue of
     * unapplied acks. In fact, if the size of unapplied acks is chosen properly, we should not
     * block here.
     */
    public void ack(PartitionOffset partitionOffset) throws InterruptedException {
        while (!unappliedAcks.offer(partitionOffset)) {
            logThrottle.logger("waiting-aks-full").error("The queue for waiting acks is full. "
                            + "Waiting... [You should never see this message. Consider increasing "
                            + "queue capacity in code. Current value: {}]",
                    unappliedAcks.size() + unappliedAcks.remainingCapacity());

            Thread.sleep(1);
        }
    }

    /**
     * @return the size of consumer records which are polled but not yet delivered to the client.
     */
    public int queuedRecordsSize() {
        return queuedRecords.size();
    }

    /**
     * @return the size of acks which are received from client but not yet applied to the
     * {@link OffsetTracker}.
     */
    public int unappliedAcksSize() {
        return unappliedAcks.size();
    }

    @Override
    public void close() {
        stop = true;
        thread.interrupt();
        kafkaConsumer.wakeup();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new AssertionError("Unexpected interrupt.", e);
        }
        try {
            kafkaConsumer.close();
        } finally {
            if (reporter != null) {
                reporter.close();
            }
        }
    }


    private Thread initConsumerThread() {
        String threadName = "Kafka reader";
        Thread thread = new Thread(() -> {
            logger.info(threadName + " started.");

            // Continuously both handle acks and poll for new records.
            while (!stop) {
                handleAcks();
                try {
                    putRecordsInQueue(kafkaConsumer.poll(POLL_TIMEOUT_MILLIS));
                } catch (WakeupException | InterruptException | InterruptedException e) {
                    if (!stop) {
                        throw new IllegalStateException("Unexpected interrupt.");
                    }
                    logger.info(threadName + " interrupted.");
                    return;
                }
            }
        });
        thread.setName(threadName);
        return thread;
    }

    /**
     * Notifies the offset tracker of the new acks and commits if there is any safe offset
     * to commit.
     */
    private void handleAcks() {
        int size = unappliedAcks.size();
        if (size == 0) {
            return;
        }
        List<PartitionOffset> offsets = new ArrayList<>(size);
        unappliedAcks.drainTo(offsets, size);
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (PartitionOffset partitionOffset : offsets) {
            OptionalLong offsetToCommit =
                    offsetTracker.ack(partitionOffset.partition(), partitionOffset.offset());
            if (offsetToCommit.isPresent()) {
                offsetsToCommit.put(new TopicPartition(topic, partitionOffset.partition()),
                        new OffsetAndMetadata(offsetToCommit.getAsLong()));
            }
        }
        if (!offsetsToCommit.isEmpty()) {
            kafkaConsumer.commitAsync(offsetsToCommit, offsetCommitCallback);
        }
    }

    /**
     * Puts the given records in queue. Meanwhile if the queue is full, it handles new
     * received acks too.
     */
    private void putRecordsInQueue(ConsumerRecords<K, V> records) throws InterruptedException {
        for (ConsumerRecord<K, V> record : records) {

            while (!offsetTracker.track(record.partition(), record.offset())) {
                logThrottle.logger("tracker-full").error("Offset tracker for partition {} is full. "
                        + "Waiting... [You should never see this message. Consider increasing "
                        + "the max number of open pages]", record.partition());
                handleAcks();
                Thread.sleep(1);
            }

            while (!queuedRecords.offer(record)) {
                handleAcks();
                Thread.sleep(1);
            }
        }
    }

    private static void checkArgument(boolean checkResult) {
        if (!checkResult) {
            throw new IllegalArgumentException();
        }
    }

    private static void checkState(boolean checkResult, String msg) {
        if (!checkResult) {
            throw new IllegalStateException(msg);
        }
    }
}
