package com.enterprisewebservice.completion;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.SearchResult;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.enterprisewebservice.RedisSearchIndexer;
import com.enterprisewebservice.embeddings.EmbeddingData;
import com.enterprisewebservice.embeddings.EmbeddingResponse;
import com.enterprisewebservice.embeddings.EmbeddingService;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;


@ApplicationScoped
public class ArticleSearchService {
    @Inject
    EmbeddingService embeddingService;

    @Inject
    RedisSearchIndexer redisSearchIndexer;

    public String searchArticles(String keycloakSubject, EmbeddingResponse queryEmbedding, int topN) throws Exception {
    
        // Search in Redis and get the embeddings
        List<Document> results = redisSearchIndexer.vectorSimilarityQuery(keycloakSubject, queryEmbedding);

        // Get the articles from the results
        return redisSearchIndexer.getMessage(results);

    }

}
