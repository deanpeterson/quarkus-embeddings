package com.enterprisewebservice.completion;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.MultivaluedHashMap;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.enterprisewebservice.embeddings.EmbeddingResponse;
import com.enterprisewebservice.embeddings.EmbeddingService;
import com.enterprisewebservice.model.Task;
import com.enterprisewebservice.completion.CompletionRequest;

@ApplicationScoped
public class ChatService {
    @Inject
    ArticleSearchService articleSearchService;
    
    @Inject
    EmbeddingService embeddingService;

    @Inject
    @RestClient
    OpenAICompletionClient openAICompletionClient;


    public String ask(String keycloakSubject, String query, int topN) throws IOException {
        // Create embeddings for the query
        EmbeddingResponse embeddingResponse = embeddingService.generateEmbeddings(List.of(query));

        // Search for related articles
       String articles = null;
        try {
            articles = articleSearchService.searchArticles(keycloakSubject, embeddingResponse, topN);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // Generate the message
        StringBuilder message = new StringBuilder();
        message.append("You answer questions about Tasks, Goals and Notes. Use the below information to answer the subsequent question. If the answer cannot be found in the notes, write \"I could not find an answer.\"");
        
        message.append("\n\nNotes:\n\"\"\"\n").append(articles).append("\n\"\"\"");
        
        message.append("\n\nQuestion: ").append(query);

        message.append("\n\nMore Info: The Question above should be answered by giving me extra information about each of the items discussed. Don't just paste the question content back, do some research with the articles I gave you as well as your knowledge and give me a comprehensive response.");
        // Create completion request
        CompletionRequest completionRequest = new CompletionRequest();
        completionRequest.setModel("gpt-4");
        completionRequest.setMessages(Arrays.asList(
                new Message("system", "You are a helpful assistant."),
                new Message("user", message.toString())
        ));

        CompletionResponse completionResponse = openAICompletionClient.createCompletion(completionRequest);

        // Extract and return the completion
        return completionResponse.getChoices().get(0).getMessage().getContent();
    }

}
