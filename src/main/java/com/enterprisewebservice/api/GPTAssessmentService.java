package com.enterprisewebservice.api;

import com.enterprisewebservice.model.ErrorResponse;
import com.enterprisewebservice.model.GPTAssessment;
import com.enterprisewebservice.model.Note;
import com.enterprisewebservice.model.StrapiServiceResponse;
import com.enterprisewebservice.model.Tag;
import com.enterprisewebservice.model.Task;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@ApplicationScoped
public class GPTAssessmentService {

    @Inject
    ApiClient apiClient;
    
    private final ObjectMapper objectMapper = new ObjectMapper(); // replace with your actual JSON library

    public StrapiServiceResponse<List<GPTAssessment>> getAll(String keycloakSubject) throws IOException {
        if (keycloakSubject == null || keycloakSubject.isEmpty()) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Goal ID is undefined"));
        }
        String response = null;
        try {
            response = apiClient.get("/gpt-assessments?filters[userProfile][keycloaksubject][$eq]="+keycloakSubject + "&sort[0]=createdAt%3Aasc");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, Note.class)));

    }

    // public StrapiServiceResponse<Note> getNote(Long id) throws IOException {
    //     if (id == null) {
    //         return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Note ID is undefined"));
    //     }
    //     System.out.println("getting note with id: " + id);
    //     String response = null;
    //     try {
    //         response = apiClient.get("/notes/" + id + "?populate[0]=tags&populate[1]=userProfile");
    //     } catch (Exception e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }
    //     return objectMapper.readValue(response, new com.fasterxml.jackson.core.type.TypeReference<StrapiServiceResponse<Note>>(){});
    // }


    // public StrapiServiceResponse<List<Note>> getByTags(List<Tag> tags, String keycloakSubject) throws IOException {
    //     if (tags == null || tags.isEmpty()) {
    //         return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Tags are undefined"));
    //     }
    //     StringBuilder tagFilters = new StringBuilder();
    //     for (Tag tag : tags) {
    //         tagFilters.append("filters[tags][id][$in]=").append(tag.getId()).append("&");
    //     }
    //     tagFilters.deleteCharAt(tagFilters.length() - 1); // Remove the last "&"
    //     String response = null;
    //     try {
    //         response = apiClient.get("/notes?filters[userProfile][keycloaksubject][$eq]=" + keycloakSubject + "&" + tagFilters.toString());
    //     } catch (Exception e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }
    //     return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, Note.class)));

    // }

    // Note: For methods requiring POST, PUT and DELETE, you will have to implement similar methods in ApiClient and use them here.

    public StrapiServiceResponse<GPTAssessment> create(GPTAssessment gptAssessment) {
        try {
            System.out.println("creating gpt assessment: " + gptAssessment);
            String response = apiClient.post("/gpt-assessments", gptAssessment);
            GPTAssessment resultData = objectMapper.readValue(response, GPTAssessment.class);
            System.out.println("created gpt assessment: " + resultData.getId());
            return new StrapiServiceResponse<>(resultData, null);
        } catch (Exception e) {
            ErrorResponse error = new ErrorResponse(500, "Internal Server Error", e.getMessage());
            System.out.println("error creating gpt assessment: " + error.getData().toString());
            return new StrapiServiceResponse<>(null, error);
        }
    }

    public void initializeAssessment(String assessment, Task task)
    {
         
        // Save the response as a GPTAssessment
        GPTAssessment gptAssessment = new GPTAssessment();
        gptAssessment.setAssessment(assessment);
        gptAssessment.setCreateDate(new Date());
        gptAssessment.setTasks(Collections.singletonList(task));
        gptAssessment.setUserProfile(task.getUserProfile());

        create(gptAssessment);
    }
    
    

}
