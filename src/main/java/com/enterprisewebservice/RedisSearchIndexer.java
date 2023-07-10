package com.enterprisewebservice;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.ModuleCommands;
import redis.clients.jedis.exceptions.*;
import redis.clients.jedis.params.SortingParams;
import redis.clients.jedis.util.*;
import java.nio.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.enterprisewebservice.embeddings.EmbeddingData;
import com.enterprisewebservice.embeddings.EmbeddingResponse;

import jakarta.enterprise.context.ApplicationScoped;
import redis.clients.jedis.search.Schema.VectorField.VectorAlgo;
import redis.clients.jedis.search.schemafields.TextField;
import redis.clients.jedis.search.schemafields.VectorField;
import redis.clients.jedis.search.*;


public class RedisSearchIndexer {
    private JedisPooled jedis;
    private static final String VECTOR_DIM = "1536";
    private static final String VECTOR_NUMBER = "8000";
    private static final String INDEX_NAME = "embeddings-";
    //private static final String PREFIX = "goalora:";
    private static final String DISTANCE_METRIC = "COSINE";

    public RedisSearchIndexer(JedisPooled jedis) {
        this.jedis = jedis;
    }

   public void createIndex(String keycloakSubject) {

        // Define the default algorithm
        VectorField.VectorAlgorithm defaultAlgorithm = VectorField.VectorAlgorithm.FLAT;

        // Set defaultAttributes for the embedding_vector field
        Map<String, Object> defaultAttributes = new HashMap<>();
        defaultAttributes.put("TYPE", "FLOAT32");
        defaultAttributes.put("DIM", VECTOR_DIM);
        defaultAttributes.put("DISTANCE_METRIC", DISTANCE_METRIC);
        defaultAttributes.put("INITIAL_CAP", VECTOR_NUMBER);
        Schema sc = new Schema().addFlatVectorField("embedding", defaultAttributes);
        sc.addTextField("index",1.0);
        sc.addTextField("object",1.0);
        try {
            var query = new Query("*");
            jedis.ftSearch(INDEX_NAME + keycloakSubject, query);
            System.out.println("Index already exists");
        } catch (JedisDataException ex) {
            // Create the index
            jedis.ftCreate(INDEX_NAME + keycloakSubject, IndexOptions.defaultOptions(), sc);
            // jedis.ftCreate(
            //     INDEX_NAME + keycloakSubject,
            //     FTCreateParams.createParams()
            //         .on(IndexDataType.JSON)
            //         .addPrefix(keycloakSubject),
            //     TextField.of("$.index").as("index"),
            //     TextField.of("$.object").as("object"),
            //     VectorField.builder().fieldName("$.embedding").algorithm(defaultAlgorithm).attributes(defaultAttributes).build().as("embedding")
            //);
            System.out.println("Index created successfully");
        }
    }

    public void indexEmbeddings(EmbeddingResponse embeddingResponse, String customKey, String title, String description) {
        for (EmbeddingData embeddingData : embeddingResponse.getData()) {
            String key = customKey + "-" + embeddingData.getIndex();
            // Convert embeddings to byte arrays
            byte[] embedding = toByteArray(embeddingData.getEmbedding());

            // Create a new hash in Redis for the document
            Map<byte[], byte[]> hash = new HashMap<>();
            hash.put("index".getBytes(), Integer.toString(embeddingData.getIndex()).getBytes());
            hash.put("title".getBytes(), title.getBytes());
            hash.put("description".getBytes(), description.getBytes());
            hash.put("embedding".getBytes(), embedding);
            
            jedis.hset(key.getBytes(), hash);
        }
    }

    public void deleteEmbedding(String customKey) {
        for (int i = 0; i < 5; i++) {
            String key = customKey + "-" + i;
            System.out.println("Deleting key: " + key);
            jedis.del(key.getBytes());
        }
    }

    public void dropIndex(String indexName)
    {
        jedis.ftDropIndex(indexName);
    }

    public void deleteDocuments()
    {
        jedis.flushAll();
    }


    

    public Entry<SearchResult, Map<String, Object>> vectorSimilarityQuery(String keycloakSubject, EmbeddingResponse queryEmbedding) {
        // Use the "embedding" field in Redis as vectorKey
        String vectorKey = "embedding";

        // Assuming that the EmbeddingResponse contains multiple EmbeddingData 
        // each of which includes the embeddings of the query article.
        List<EmbeddingData> queryEmbeddingData = queryEmbedding.getData();

        // Convert the query vector to a byte array for each embedding and execute the query.
        Entry<SearchResult, Map<String, Object>> result = null;
        for (EmbeddingData data : queryEmbeddingData) {
            List<Float> vectorValues = data.getEmbedding();

            // Convert the query vector to a byte array
            byte[] queryVector = toByteArray(vectorValues);

            // Create a new search query
            Query searchQuery = new Query("*=>[KNN 10 @" + vectorKey + " $vec]")
                                        .addParam("vec", queryVector)
                                        .setSortBy("__" + vectorKey + "_score", false)
                                        .returnFields("title", "description", "__" + vectorKey + "_score")
                                        .dialect(2);

            // Execute the search query
            result = jedis.ftProfileSearch(INDEX_NAME + keycloakSubject, FTProfileParams.profileParams(), searchQuery);

        }

        return result;
    }

    public String getMessage(Entry<SearchResult, Map<String, Object>> searchResults) {
        // Convert the search results to a list of EmbeddingData
        StringBuffer message = new StringBuffer();
        
        // Get the list of documents and initialize an index
        List<Document> documents = searchResults.getKey().getDocuments();
        int i = 0;

        // While there are still documents and the message doesn't exceed the limit
        while (i < documents.size() && message.length() < 7500) {
            Document doc = documents.get(i);
            
            // Get the index, object, and embedding from the document
            String title = "article 1: " + doc.getString("title");
            String description = "description: " + doc.getString("description");
            String part = title + "\n" + description + "\n";
            
            // Check if adding the next part would exceed the limit
            if (message.length() + part.length() <= 7500) {
                // If it wouldn't, append the part
                message.append(part);
            }

            // Increment the index
            i++;
        }
        
        return message.toString();
    }


    private byte[] toByteArray(List<Float> list) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * list.size());
        for (float value : list) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }
}
