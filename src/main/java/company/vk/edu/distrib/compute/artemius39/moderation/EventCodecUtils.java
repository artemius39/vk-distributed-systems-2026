package company.vk.edu.distrib.compute.artemius39.moderation;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.regex.Pattern;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

final class EventCodecUtils {
    private static final String SEPARATOR = "\n";
    private static final Pattern LINK_ID = Pattern.compile("[a-zA-Z0-9]{10}");

    private EventCodecUtils() {
        // utility class
    }

    static byte[] key(String id) {
        return id.getBytes(StandardCharsets.UTF_8);
    }

    static byte[] encodeCreated(LinkCreatedEvent event) {
        return String.join(SEPARATOR, event.id(), event.createdAt().toString(), event.url())
            .getBytes(StandardCharsets.UTF_8);
    }

    static LinkCreatedEvent decodeCreated(byte[] key, byte[] value) throws CharacterCodingException {
        String[] fields = decode(value).split(SEPARATOR, 3);
        if (fields.length != 3 || fields[2].isBlank()) {
            throw new IllegalArgumentException("Invalid link creation event");
        }
        validateKey(key, fields[0]);
        return new LinkCreatedEvent(fields[0], fields[2], Instant.parse(fields[1]));
    }

    static byte[] encodeResult(ModerationResultEvent event) {
        return String.join(SEPARATOR, event.id(), event.passed().toString(),
            event.moderatedAt().toString(), event.reason()).getBytes(StandardCharsets.UTF_8);
    }

    static ModerationResultEvent decodeResult(byte[] key, byte[] value) throws CharacterCodingException {
        String[] fields = decode(value).split(SEPARATOR, 4);
        if (fields.length != 4 || !("true".equals(fields[1]) || "false".equals(fields[1]))) {
            throw new IllegalArgumentException("Invalid moderation result");
        }
        validateKey(key, fields[0]);
        return new ModerationResultEvent(fields[0], Boolean.valueOf(fields[1]), fields[3], Instant.parse(fields[2]));
    }

    private static String decode(byte[] value) throws CharacterCodingException {
        if (value == null) {
            throw new IllegalArgumentException("Missing event payload");
        }
        return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(value)).toString();
    }

    private static void validateKey(byte[] key, String id) {
        if (!LINK_ID.matcher(id).matches() || !Arrays.equals(key, key(id))) {
            throw new IllegalArgumentException("Invalid event key");
        }
    }
}
