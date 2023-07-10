package com.enterprisewebservice.api;

import com.enterprisewebservice.embeddings.ChunkingService;
import com.enterprisewebservice.embeddings.EmbeddingService;
import com.enterprisewebservice.embeddings.EmbeddingResponse;
import com.enterprisewebservice.model.StrapiServiceResponse;
import com.enterprisewebservice.model.Note;
import com.enterprisewebservice.model.Task;
import com.enterprisewebservice.RedisSearchIndexer;
import com.enterprisewebservice.completion.ArticleSearchService;
import com.enterprisewebservice.completion.ChatService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class NoteServiceTest {

    @Inject
    NoteService noteService;

    @Inject
    TaskService taskService;

    @Inject
    EmbeddingService embeddingService;

    @Inject
    RedisSearchIndexer redisSearchIndexer;

    @Inject
    ArticleSearchService articleSearchService;

    @Inject
    ChatService chatService;

    // @Test
    // public void testNoteEmbedding() {
    //     try {
    //         // Replace with your specific Note ID
    //         Integer noteId = 7;
    //         String keycloakSubject = "efce8989-2d29-42ae-9f38-092dfe9f14c6";
    //         StrapiServiceResponse<List<Note>> result = noteService.getAll(keycloakSubject);

    //         assertNotNull(result);
    //         assertNull(result.getError());
            
    //          // Create the index
    //         redisSearchIndexer.createIndex(keycloakSubject);
    //         result.getData().forEach(note -> {
    //             assertNotNull(note);
                
    //             try {
                    
    //                 EmbeddingResponse embeddingResponse = embeddingService.generateEmbeddings(ChunkingService.chunkObject(note));

    //                 assertNotNull(embeddingResponse);
    //                 assertNotNull(embeddingResponse.getData());
    //                 assertTrue(embeddingResponse.getData().size() > 0);
    //                 // Index the embeddings
    //                 redisSearchIndexer.indexEmbeddings(keycloakSubject, embeddingResponse, note.getId().toString(), note.getName(), note.getRichText());
    //             } catch (Exception e) {
    //                 // TODO Auto-generated catch block
    //                 e.printStackTrace();
    //             }
                
    //         });

    //         // Additional checks can be added based on the response.
    //         // For example, check if data returned matches expected data
    //         // Assertions.assertEquals("expectedNoteData", note);
    //     } catch (Exception e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }
    // }

    // @Test
    // public void testAskQuestion()
    // {
    //     String keycloakSubject = "efce8989-2d29-42ae-9f38-092dfe9f14c6";
    //     try {
    //         StrapiServiceResponse<Task> result = taskService.getTask(31);
    //         String query = "Can you help me complete the following task with name: " + (result.getData() != null && result.getData().getTitle() != null ? result.getData().getTitle() : "") + " and description: " + (result.getData() != null && result.getData().getDescription() != null ? result.getData().getDescription() : "");
           
    //         assertNotNull(result);
    //         assertNull(result.getError());

    //         String answer = chatService.ask(keycloakSubject,query, 3);

    //         assertNotNull(answer);
    //         System.out.println(answer);
    //     } catch (IOException e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }
    // }
}
