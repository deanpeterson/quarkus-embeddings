package com.enterprisewebservice.api;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.enterprisewebservice.model.StrapiServiceResponse;
import com.enterprisewebservice.model.Goal;

import java.io.IOException;
import java.util.List;


@QuarkusTest
public class GoalServiceTest {

    @Inject
    GoalService goalService;

    @Test
    public void testGetAll() {
        StrapiServiceResponse<List<Goal>> result = null;
        try {
            result = goalService.getAll("efce8989-2d29-42ae-9f38-092dfe9f14c6");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.getData());
        Assertions.assertNull(result.getError());

        // Additional checks can be added based on the response. 
        // For example, check if data returned matches expected data
        // Assertions.assertEquals("expectedGoalData", result.getData());
    }
}

