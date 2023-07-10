package com.enterprisewebservice.embeddings;

import org.eclipse.microprofile.rest.client.inject.RestClient;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.util.List;

@ApplicationScoped
public class EmbeddingService {
    @Inject
    @RestClient
    OpenAIEmbeddingClient openAIEmbeddingClient;

    public EmbeddingResponse generateEmbeddings(List<String> texts) {
        EmbeddingRequest request = new EmbeddingRequest();
        request.setModel("text-embedding-ada-002");
        request.setInput(texts);

        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("Authorization", "Bearer xxx-openapi-key-xxx");

        EmbeddingResponse response = openAIEmbeddingClient.createEmbedding(request);
        // Handle the response, save embeddings to Redis, etc...
        return response;
    }
}

