package com.enterprisewebservice.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = GoalStatusDeserializer.class)
public enum GoalStatus {
    NOT_STARTED("not started"),
    IN_PROGRESS("in progress"),
    COMPLETED("completed");

    private final String value;

    GoalStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static GoalStatus fromValue(String value) {
        for (GoalStatus goalStatus : values()) {
            if (goalStatus.getValue().equalsIgnoreCase(value)) {
                return goalStatus;
            }
        }
        throw new IllegalArgumentException("Unknown enum string value: " + value);
    }
}
