package company.vk.edu.distrib.compute.artemius39.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class ModerationServiceFactoryImpl implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new ModerationServiceImpl(bootstrapServers, loadBannedWords());
    }

    private List<String> loadBannedWords() throws IOException {
        try (InputStream stream = getClass().getResourceAsStream("/moderation/banned_words.txt")) {
            if (stream == null) {
                throw new IOException("Banned words dictionary not found");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                List<String> words = new ArrayList<>();
                String line = reader.readLine();
                while (line != null) {
                    String word = line.strip().toLowerCase(Locale.ROOT);
                    if (!word.isEmpty()) {
                        words.add(word);
                    }
                    line = reader.readLine();
                }
                return words;
            }
        }
    }
}
