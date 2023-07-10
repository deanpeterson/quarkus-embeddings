package com.enterprisewebservice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StrapiServiceResponse<T> {
    private T data;
    private Wrapper<T> wrappedData;
    private ErrorResponse error;

    public StrapiServiceResponse() {}

    public StrapiServiceResponse(T data, ErrorResponse error) {
        this.data = data;
        this.error = error;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Wrapper<T> getWrappedData() {
        return wrappedData;
    }

    public void setWrappedData(Wrapper<T> wrappedData) {
        this.wrappedData = wrappedData;
    }

    public ErrorResponse getError() {
        return error;
    }

    public void setError(ErrorResponse error) {
        this.error = error;
    }

    public static class Wrapper<T> {
        private T data;

        public Wrapper() {}

        public Wrapper(T data) {
            this.data = data;
        }

        public T getData() {
            return data;
        }

        public void setData(T data) {
            this.data = data;
        }
    }
}
