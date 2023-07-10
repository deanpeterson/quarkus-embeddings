package com.enterprisewebservice.embeddings;

import java.util.List;

public class EmbeddingRequest {
    private String model;
    private List<String> input;
    
    public String getModel() {
        return model;
    }
    public void setModel(String model) {
        this.model = model;
    }
    public List<String> getInput() {
        return input;
    }
    public void setInput(List<String> input) {
        this.input = input;
    }
}
