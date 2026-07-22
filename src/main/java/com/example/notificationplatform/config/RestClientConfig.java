package com.example.notificationplatform.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {
    @Bean
    public RestClient webhookRestClient(RestClient.Builder builder){
        return builder.build();
    }
}
