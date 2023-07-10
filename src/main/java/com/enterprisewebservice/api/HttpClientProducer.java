package com.enterprisewebservice.api;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.ws.rs.core.MediaType;
import java.net.http.HttpClient;

@ApplicationScoped
public class HttpClientProducer {

    @Produces
    public HttpClient produceHttpClient() {
        return HttpClient.newHttpClient();
    }
}

