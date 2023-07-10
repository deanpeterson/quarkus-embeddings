package com.enterprisewebservice.embeddings;

import java.util.List;

public class EmbeddingData {
    private int index;
    private List<Float> embedding;
    private String object;
    
    public EmbeddingData(int index, String object, List<Float> embedding) {
        this.index = index; 
        this.embedding = embedding;
        this.object = object;
    }
    public String getObject() {
        return object;
    }
    public void setObject(String object) {
        this.object = object;
    }
    public int getIndex() {
        return index;
    }
    public void setIndex(int index) {
        this.index = index;
    }
    public List<Float> getEmbedding() {
        return embedding;
    }
    public void setEmbedding(List<Float> embedding) {
        this.embedding = embedding;
    }
}
