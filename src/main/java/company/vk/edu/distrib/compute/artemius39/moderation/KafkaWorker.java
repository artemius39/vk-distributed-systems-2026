package company.vk.edu.distrib.compute.artemius39.moderation;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class KafkaWorker {
    private static final Logger log = LoggerFactory.getLogger(KafkaWorker.class);
    protected static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration RETRY_DELAY = Duration.ofMillis(500);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(2);

    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean();
    private final Thread worker;

    protected KafkaWorker(String bootstrapServers, String groupId, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.worker = new Thread(this::run, groupId);
    }

    public final void start() {
        running.set(true);
        worker.start();
    }

    public final void stop() {
        running.set(false);
        if (!worker.isAlive()) {
            return;
        }
        try {
            if (!worker.join(STOP_TIMEOUT)) {
                worker.interrupt();
                worker.join();
            }
        } catch (InterruptedException e) {
            worker.interrupt();
            Thread.currentThread().interrupt();
        }
    }

    protected final boolean isRunning() {
        return running.get();
    }

    private void run() {
        try {
            while (isRunning()) {
                try {
                    runSession();
                } catch (RuntimeException | IOException | ExecutionException e) {
                    if (isRunning()) {
                        log.warn("Kafka worker {} failed, retrying", groupId, e);
                        Thread.sleep(RETRY_DELAY);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
        }
    }

    private void runSession() throws IOException, ExecutionException, InterruptedException {
        try (KafkaProducer<byte[], byte[]> producer = createProducer();
             KafkaConsumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(List.of(topic));
            while (isRunning()) {
                process(producer, consumer);
            }
        }
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        return new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000,
            ProducerConfig.ACKS_CONFIG, "all"
        ), new ByteArraySerializer(), new ByteArraySerializer());
    }

    private KafkaConsumer<byte[], byte[]> createConsumer() {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
            ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000,
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50
        ), new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    protected static void commit(KafkaConsumer<byte[], byte[]> consumer, ConsumerRecord<byte[], byte[]> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)));
    }

    protected abstract void process(KafkaProducer<byte[], byte[]> producer, KafkaConsumer<byte[], byte[]> consumer)
        throws IOException, ExecutionException, InterruptedException;
}
