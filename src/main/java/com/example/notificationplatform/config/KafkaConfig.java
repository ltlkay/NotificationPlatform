package com.example.notificationplatform.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.template.default-topic}")
    private String defaultTopicName;

    @Value("${spring.kafka.template.dlq-topic}")
    private String dlqTopicName;

    @Bean
    public NewTopic notificationTopic() {
        return TopicBuilder.name(defaultTopicName)
                .partitions(3)
                .build();
    }

    @Bean NewTopic dlqTopic() {
        return TopicBuilder.name(dlqTopicName)
                .partitions(3)
                .build();
    }
}
