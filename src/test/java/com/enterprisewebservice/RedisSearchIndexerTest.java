package com.enterprisewebservice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.IndexOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
public class RedisSearchIndexerTest {

    @Inject
    RedisSearchIndexer redisSearchIndexer;

    // @BeforeEach
    // public void setUp() {
    //     redisSearchIndexer = new RedisSearchIndexer("192.168.1.10");
    // }

    // @Test
    // public void createIndex() {
    //     assertDoesNotThrow(() -> indexer.createIndex());
    // }

    // @Test
    // public void indexDocuments() {
    //     List<Float> testVector = new ArrayList<>(Arrays.asList(0.1f, 0.2f, 0.3f, 0.4f));
    //     HashMap<String, Object> fields = new HashMap<>();

    //     fields.put("title", "Test Title 2");
    //     fields.put("url", "https://example.com");
    //     fields.put("text", "Test Text");
    //     fields.put("title_vector", testVector);
    //     fields.put("content_vector", testVector);

    //     Document document = new Document("3", fields);

    //     assertDoesNotThrow(() -> indexer.indexDocuments(Arrays.asList(document)));
    // }

    // @Test
    // public void flushAll() throws Exception {
    //     redisSearchIndexer.deleteDocuments();
    // }
}

