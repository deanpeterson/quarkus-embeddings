package com.enterprisewebservice.model;

public class ErrorResponse {
    private int status;
    private String statusText;
    private Object data;

    public ErrorResponse() {}

    public ErrorResponse(int status, String statusText, Object data) {
        this.status = status;
        this.statusText = statusText;
        this.data = data;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getStatusText() {
        return statusText;
    }

    public void setStatusText(String statusText) {
        this.statusText = statusText;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
