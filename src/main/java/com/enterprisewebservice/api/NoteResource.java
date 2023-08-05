package com.enterprisewebservice.api;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.GET;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import com.enterprisewebservice.api.NoteService;
import com.enterprisewebservice.model.StrapiServiceResponse;
import com.enterprisewebservice.model.Task;
import com.enterprisewebservice.model.TaskNote;
import com.enterprisewebservice.embeddings.EmbeddingService;
import com.enterprisewebservice.model.GPTAssessment;
import com.enterprisewebservice.model.Note;
import com.enterprisewebservice.model.StrapiEventPayload;
import com.enterprisewebservice.embeddings.EmbeddingResponse;
import com.enterprisewebservice.embeddings.ChunkingService;

import com.enterprisewebservice.RedisSearchIndexer;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.instrument.MeterRegistry;

@Path("/notes")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NoteResource {

    @Inject
    NoteService noteService;

    @Inject
    RedisSearchIndexer redisSearchIndexer;
    
    @Inject
    EmbeddingService embeddingService;

    @Inject
    TaskNoteService taskNoteService;

    @Inject
    MeterRegistry registry;

    @Inject
    TaskService taskService;

    @Inject
    GPTAssessmentService gptAssessmentService;

    @GET
    @Path("/test")
    public Response getTestValue() {
        return Response.ok("Test value").build();
    }

    @POST
    @Path("/create-assessment")
    public Response createAssessment(StrapiEventPayload payload) {
        try {
            System.out.println("Payload model in create-assessment: " + payload.getModel().toString());
            if (payload == null || !payload.getModel().equals("task-note")) {
                return Response.status(Response.Status.BAD_REQUEST).entity("Payload is not a task note").build();
            }
            System.out.println("Payload: " + payload.toString());
            Long id = payload.getEntry().getId();
            StrapiServiceResponse<TaskNote> taskNoteData = taskNoteService.getTaskNote(id);
            TaskNote taskNote = taskNoteData.getData();
            Task task = taskNote.getTask();
            String assessment = taskService.askQuestion(task.getId().intValue(), task.getUserProfile().getKeycloaksubject());
            gptAssessmentService.initializeAssessment(assessment, task);

        } catch (Exception e) {
            // Handle exception appropriately
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
         return Response.ok().build(); 
    }

    @POST
    @Counted(value = "note_create_embedding")
    @Path("/create-embedding")
    public Response getNoteAndIndex(StrapiEventPayload payload) {
        try {
           if (payload == null || !payload.getModel().equals("note")) {
                return Response.status(Response.Status.BAD_REQUEST).entity("Payload is not a note").build();
            }
           System.out.println("Payload: " + payload.toString());
            Long id = payload.getEntry().getId();

            StrapiServiceResponse<Note> result = noteService.getNote(id);

            // Assuming the object has a method for getting the data
            Note note = result.getData();

            String keycloakSubject = result.getData().getUserProfile().getKeycloaksubject();

            // Create the index
            redisSearchIndexer.createIndex();

            EmbeddingResponse embeddingResponse = embeddingService.generateEmbeddings(ChunkingService.chunkObject(note));

            // Index the embeddings
            redisSearchIndexer.indexEmbeddings(embeddingResponse, note.getId().toString(), note.getName(), note.getRichText(), keycloakSubject);

             // Fetch the TaskNote objects by Note id
            StrapiServiceResponse<List<TaskNote>> taskNotes = taskNoteService.getByNoteId(note.getId().toString(), false);
            List<Task> uniqueTasks = new ArrayList<>();
            for (TaskNote taskNote : taskNotes.getData()) {
                if (!uniqueTasks.contains(taskNote.getTask())) {
                    uniqueTasks.add(taskNote.getTask());
                }
            }

            System.out.println("Unique tasks size: " + uniqueTasks.size());
            // Ask a question for every unique Task
            for (Task task : uniqueTasks) {
                System.out.println("Task id generating assessment for: " + task.getId());
                String assessment = taskService.askQuestion(task.getId().intValue(), keycloakSubject);
                System.out.println("Initialize Assessment: " + assessment);
                gptAssessmentService.initializeAssessment(assessment, task);
            }

            return Response.ok(note).build(); // returns the note object

        } catch (Exception e) {
            // Handle exception appropriately
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

    @POST
    @Path("/delete-embedding")
    public Response deleteNoteIndex(StrapiEventPayload payload) {
        try {
           if (payload == null || !payload.getModel().equals("note")) {
                return Response.status(Response.Status.BAD_REQUEST).entity("Payload is not a note").build();
            }
            System.out.println("Payload: " + payload.toString());
            Long id = payload.getEntry().getId();
            

            // Assuming the object has a method for getting the data
           
            // Index the embeddings
            redisSearchIndexer.deleteEmbedding(id.toString());

            return Response.ok(id).build(); // returns the note object

        } catch (Exception e) {
            // Handle exception appropriately
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

    @DELETE
    @Path("/delete-all-embeddings")
    public Response deleteEmbeddings() {
        try {
           
            redisSearchIndexer.deleteDocuments();
            return Response.ok().build(); // returns the note object

        } catch (Exception e) {
            // Handle exception appropriately
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }
}
