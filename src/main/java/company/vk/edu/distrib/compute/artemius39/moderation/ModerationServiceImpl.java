package company.vk.edu.distrib.compute.artemius39.moderation;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

final class ModerationServiceImpl extends KafkaWorker implements ModerationService {
    private final List<String> bannedWords;

    ModerationServiceImpl(String bootstrapServers, List<String> bannedWords) {
        super(bootstrapServers, "artemius39-moderation", KafkaTopics.LINKS_CREATED);
        this.bannedWords = List.copyOf(bannedWords);
    }

    @Override
    protected void process(KafkaProducer<byte[], byte[]> producer, KafkaConsumer<byte[], byte[]> consumer)
        throws ExecutionException, InterruptedException {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL_TIMEOUT)) {
            if (!isRunning()) {
                return;
            }
            ProducerRecord<byte[], byte[]> response = moderate(record);
            producer.send(response).get();
            commit(consumer, record);
        }
    }

    private ProducerRecord<byte[], byte[]> moderate(ConsumerRecord<byte[], byte[]> record) {
        try {
            LinkCreatedEvent event = EventCodecUtils.decodeCreated(record.key(), record.value());
            String reason = rejectionReason(event.url());
            ModerationResultEvent result = new ModerationResultEvent(
                event.id(), reason.isEmpty(), reason, Instant.now()
            );
            return new ProducerRecord<>(
                KafkaTopics.LINKS_MODERATED, record.key(), EventCodecUtils.encodeResult(result)
            );
        } catch (IllegalArgumentException | DateTimeException | CharacterCodingException e) {
            return deadLetter(record, e);
        }
    }

    private String rejectionReason(String url) {
        URI uri = URI.create(url);
        if (uri.getHost() == null || !("http".equalsIgnoreCase(uri.getScheme())
            || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new IllegalArgumentException("Invalid URL");
        }
        String decoded = URLDecoder.decode(url.replace("+", "%2B"), StandardCharsets.UTF_8)
            .toLowerCase(Locale.ROOT);
        for (String word : bannedWords) {
            if (decoded.contains(word)) {
                return "Banned word: " + word;
            }
        }
        return "";
    }

    private static ProducerRecord<byte[], byte[]> deadLetter(ConsumerRecord<byte[], byte[]> record, Exception error) {
        ProducerRecord<byte[], byte[]> response = new ProducerRecord<>(
            KafkaTopics.LINKS_CREATED_DLT, null, record.key(), record.value(), record.headers()
        );
        response.headers().add("error", error.toString().getBytes(StandardCharsets.UTF_8));
        return response;
    }
}
