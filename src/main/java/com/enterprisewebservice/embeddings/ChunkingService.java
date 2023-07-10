package com.enterprisewebservice.embeddings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.enterprisewebservice.model.Goal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ChunkingService {
    private static final int MAX_TOKENS = 1600;
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(SerializationFeature.INDENT_OUTPUT, true);

    public static List<String> chunkObject(Object obj) throws Exception {
        String text = objectMapper.writeValueAsString(obj);
        return splitIntoChunks(text, MAX_TOKENS);
    }

    private static List<String> splitIntoChunks(String text, int maxTokens) {
        List<String> words = new ArrayList<>(Arrays.asList(text.split("\\s+")));
        List<String> chunks = new ArrayList<>();
        List<String> currentChunk = new ArrayList<>();

        for (String word : words) {
            currentChunk.add(word);

            if (currentChunk.size() >= maxTokens) {
                chunks.add(String.join(" ", currentChunk));
                currentChunk.clear();
            }
        }

        // Add the last chunk if it's not empty
        if (!currentChunk.isEmpty()) {
            chunks.add(String.join(" ", currentChunk));
        }

        return chunks;
    }

    public static void main(String[] args) throws Exception {
        // Usage:
        Goal exampleGoal = new Goal();
        // Fill your goal object with data

        List<String> chunks = chunkObject(exampleGoal);
        System.out.println(chunks);
    }
}
