package com.enterprisewebservice.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = TaskStatusDeserializer.class)
public enum TaskStatus {
    NOT_STARTED("not started"),
    IN_PROGRESS("in progress"),
    COMPLETED("completed");

    private final String value;

    TaskStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static TaskStatus fromValue(String value) {
        for (TaskStatus taskStatus : values()) {
            if (taskStatus.getValue().equalsIgnoreCase(value)) {
                return taskStatus;
            }
        }
        throw new IllegalArgumentException("Unknown enum string value: " + value);
    }
}
