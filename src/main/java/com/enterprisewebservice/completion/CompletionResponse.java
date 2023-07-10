package com.enterprisewebservice.completion;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompletionResponse {
    // define properties here as per the response structure
    // assume it has a Choice object

    private List<Choice> choices;

    // getters and setters

    public List<Choice> getChoices() {
        return choices;
    }

    public void setChoices(List<Choice> choices) {
        this.choices = choices;
    }
}

