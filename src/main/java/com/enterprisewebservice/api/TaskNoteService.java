package com.enterprisewebservice.api;

import com.enterprisewebservice.model.ErrorResponse;
import com.enterprisewebservice.model.Note;
import com.enterprisewebservice.model.TaskNote;
import com.enterprisewebservice.model.StrapiServiceResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;

@ApplicationScoped
public class TaskNoteService {

    @Inject
    ApiClient apiClient;

    private final ObjectMapper objectMapper = new ObjectMapper(); // replace with your actual JSON library

    public StrapiServiceResponse<List<TaskNote>> getByNoteId(String id, boolean showCompleted) throws IOException {
        if (id == null || id.isEmpty()) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Goal ID is undefined"));
        }

        String response = null;
        try {
            response = apiClient.get("/task-notes?populate[0]=task&populate[1]=note&populate[2]=task.userProfile&populate[3]=task.status&filters[note][id][$eq]=" + id);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, TaskNote.class)));
    }

    public StrapiServiceResponse<List<TaskNote>> getByTaskId(String id) throws IOException {
        if (id == null || id.isEmpty()) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Task ID is undefined"));
        }

        String response = null;;
        try {
            response = apiClient.get("/task-notes?populate[0]=task&populate[1]=note&filters[task][id][$eq]=" + id);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, objectMapper.getTypeFactory().constructParametricType(StrapiServiceResponse.class, objectMapper.getTypeFactory().constructCollectionType(List.class, TaskNote.class)));
    }

    public StrapiServiceResponse<TaskNote> getByNoteIdAndTaskId(String noteId, String taskId) throws IOException {
        if (noteId == null || noteId.isEmpty() || taskId == null || taskId.isEmpty()) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Note ID or Task ID is undefined"));
        }

        String response = null;
        try {
            response = apiClient.get("/task-notes?populate[0]=task&populate[1]=note&filters[note][id][$eq]=" + noteId + "&filters[task][id][$eq]=" + taskId);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, new com.fasterxml.jackson.core.type.TypeReference<StrapiServiceResponse<TaskNote>>(){});
    }

    public StrapiServiceResponse<TaskNote> create(TaskNote taskNote) throws Exception {
        if (taskNote == null) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "TaskNote is null"));
        }

        String response = apiClient.post("/task-notes?populate=*", taskNote);
        return objectMapper.readValue(response, new com.fasterxml.jackson.core.type.TypeReference<StrapiServiceResponse<TaskNote>>(){});
    }

     public StrapiServiceResponse<TaskNote> getTaskNote(Long id) throws IOException {
        if (id == null) {
            return new StrapiServiceResponse<>(null, new ErrorResponse(400, "Bad Request", "Note ID is undefined"));
        }
        System.out.println("getting note with id: " + id);
        String response = null;
        try {
            response = apiClient.get("/task-notes/" + id + "?populate[0]=task&populate[1]=note&populate[2]=task.userProfile");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return objectMapper.readValue(response, new com.fasterxml.jackson.core.type.TypeReference<StrapiServiceResponse<TaskNote>>(){});
    }
   
}
