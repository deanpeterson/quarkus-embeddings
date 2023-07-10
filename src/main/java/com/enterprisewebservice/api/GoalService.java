package com.enterprisewebservice.api;

import com.enterprisewebservice.model.ErrorResponse;
import com.enterprisewebservice.model.Goal;
import com.enterprisewebservice.model.StrapiServiceResponse;
import com.enterprisewebservice.model.Tag;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;

@ApplicationScoped
public class GoalService {

    @Inject
    ApiClient apiClient;
    
    private final ObjectMapper objectMapper = new ObjectMapper(); // replace with your actual JSON library

    public StrapiServiceResponse<List<Goal>> getAll(String keycloakSubject) throws IOException {
        if (keycloakSubject == null || keycloakSubject.isEmpty()) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Goal ID is undefined"));
        }
        String response = null;
        try {
            response = apiClient.get("/goals?filters[userProfile][keycloaksubject][$eq]=" + keycloakSubject);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, Goal.class)));

    }

    public StrapiServiceResponse<Goal> getGoal(Integer id) throws IOException {
        if (id == null) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Goal ID is undefined"));
        }
        String response = null;
        try {
            response = apiClient.get("/goals?populate[0]=tags&populate[1]=userProfile");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, StrapiServiceResponse.class);
    }

    public StrapiServiceResponse<List<Goal>> getByTags(List<Tag> tags, String keycloakSubject) throws IOException {
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
            response = apiClient.get("/goals?filters[userProfile][keycloaksubject][$eq]=" + keycloakSubject + "&" + tagFilters.toString());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, Goal.class)));

    }

    // Note: For methods requiring POST, PUT and DELETE, you will have to implement similar methods in ApiClient and use them here.
}
