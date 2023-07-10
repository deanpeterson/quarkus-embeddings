package com.enterprisewebservice.completion;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Choice {
    // define properties here as per the response structure
    // assume it has a Message object

    private Message message;

    // getters and setters

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}

