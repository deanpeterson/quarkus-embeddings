package com.enterprisewebservice.api;

import com.enterprisewebservice.completion.ChatService;
import com.enterprisewebservice.model.ErrorResponse;
import com.enterprisewebservice.model.Task;
import com.enterprisewebservice.model.StrapiServiceResponse;
import com.enterprisewebservice.model.Tag;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;

@ApplicationScoped
public class TaskService {

    @Inject
    ApiClient apiClient;

    @Inject
    ChatService chatService;
    
    private final ObjectMapper objectMapper = new ObjectMapper(); // replace with your actual JSON library

    public StrapiServiceResponse<List<Task>> getAll(String keycloakSubject) throws IOException {
        if (keycloakSubject == null || keycloakSubject.isEmpty()) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Goal ID is undefined"));
        }
        String response = null;
        try {
            response = apiClient.get("/tasks?filters[userProfile][keycloaksubject][$eq]=" + keycloakSubject);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, Task.class)));

    }

    public StrapiServiceResponse<Task> getTask(Integer id) throws IOException {
        if (id == null) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Note ID is undefined"));
        }
        String response = null;
        try {
            response = apiClient.get("/tasks/"+id+"?populate[0]=tags&populate[1]=userProfile");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, new TypeReference<StrapiServiceResponse<Task>>() {});

    }

    public StrapiServiceResponse<List<Task>> getByTags(List<Tag> tags, String keycloakSubject) throws IOException {
        if (tags == null || tags.isEmpty()) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Tags are undefined"));
        }
        StringBuilder tagFilters = new StringBuilder();
        for (Tag tag : tags) {
            tagFilters.append("filters[tags][id][$in]=").append(tag.getId()).append("&");
        }
        tagFilters.deleteCharAt(tagFilters.length() - 1); // Remove the last "&"
        String response = null;
        try {
            response = apiClient.get("/notes?filters[userProfile][keycloaksubject][$eq]=" + keycloakSubject + "&" + tagFilters.toString());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, Task.class)));

    }

    public String askQuestion(Integer id, String keycloakSubject) throws IOException {
        StrapiServiceResponse<Task> result = getTask(id);
        String query = "Can you help me complete the following task with name: " 
            + (result.getData() != null && result.getData().getTitle() != null ? result.getData().getTitle() : "") 
            + " and description: " 
            + (result.getData() != null && result.getData().getDescription() != null ? result.getData().getDescription() : "");

        return chatService.ask(keycloakSubject, query, 3);
    }

    // Note: For methods requiring POST, PUT and DELETE, you will have to implement similar methods in ApiClient and use them here.
}
