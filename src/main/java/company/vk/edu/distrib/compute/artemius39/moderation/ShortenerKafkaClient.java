package company.vk.edu.distrib.compute.artemius39.moderation;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.time.DateTimeException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShortenerKafkaClient extends KafkaWorker {
    private static final Logger log = LoggerFactory.getLogger(ShortenerKafkaClient.class);
    private static final int QUEUE_CAPACITY = 1024;

    private final Dao<String> linkDao;
    private final BlockingQueue<LinkCreatedEvent> pending = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    public ShortenerKafkaClient(String bootstrapServers, int port, Dao<String> linkDao) {
        super(bootstrapServers, "artemius39-shortener-" + port, KafkaTopics.LINKS_MODERATED);
        this.linkDao = linkDao;
    }

    public void publish(LinkCreatedEvent event) {
        if (!pending.offer(event) && log.isWarnEnabled()) {
            log.warn("Moderation queue is full, link {} was not sent", event.id());
        }
    }

    @Override
    protected void process(KafkaProducer<byte[], byte[]> producer, KafkaConsumer<byte[], byte[]> consumer)
        throws IOException, ExecutionException, InterruptedException {
        LinkCreatedEvent event = pending.peek();
        if (event != null) {
            producer.send(new ProducerRecord<>(KafkaTopics.LINKS_CREATED,
                EventCodecUtils.key(event.id()), EventCodecUtils.encodeCreated(event))).get();
            pending.remove();
        }
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL_TIMEOUT)) {
            if (!isRunning()) {
                return;
            }
            applyResult(record);
            commit(consumer, record);
        }
    }

    private void applyResult(ConsumerRecord<byte[], byte[]> record) throws IOException {
        ModerationResultEvent result;
        try {
            result = EventCodecUtils.decodeResult(record.key(), record.value());
        } catch (IllegalArgumentException | DateTimeException | CharacterCodingException e) {
            if (log.isWarnEnabled()) {
                log.warn("Invalid moderation result at offset {}", record.offset(), e);
            }
            return;
        }
        if (!result.passed()) {
            linkDao.delete(result.id());
        }
    }
}
