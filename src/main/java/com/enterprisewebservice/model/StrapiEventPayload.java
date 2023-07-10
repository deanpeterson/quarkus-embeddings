package com.enterprisewebservice.model;

public class StrapiEventPayload {
    private String event;
    private String createdAt;
    private String model;
    private Note entry;

    public String getEvent() {
        return event;
    }
    public void setEvent(String event) {
        this.event = event;
    }
    public String getCreatedAt() {
        return createdAt;
    }
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }
    public String getModel() {
        return model;
    }
    public void setModel(String model) {
        this.model = model;
    }
    public Note getEntry() {
        return entry;
    }
    public void setEntry(Note entry) {
        this.entry = entry;
    }

    @Override
    public String toString() {
        return "StrapiEventPayload [createdAt=" + createdAt + ", entry=" + entry + ", event=" + event + ", model="
                + model + "]";
    }
    
}