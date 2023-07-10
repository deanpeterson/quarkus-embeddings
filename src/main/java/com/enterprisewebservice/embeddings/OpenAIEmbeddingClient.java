package com.enterprisewebservice.embeddings;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;

@ApplicationScoped
@RegisterRestClient(configKey="openai.api")
public interface OpenAIEmbeddingClient {

    @POST
    @Path("/embeddings")
    @ClientHeaderParam(name="Authorization", value="{generateAuthHeader}")
    EmbeddingResponse createEmbedding(EmbeddingRequest request);

    default String generateAuthHeader() {
        return "Bearer xxx-openapi-key-xxx";
    }

}

