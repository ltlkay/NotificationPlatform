package com.example.notificationplatform.event;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {
    private final KafkaTemplate<String,NotificationEvent> kafkaTemplate;

    @Value("${spring.kafka.template.default-topic}")
    private String topic;

    public void send(NotificationEvent event) {
        CompletableFuture<SendResult<String, NotificationEvent>> future = kafkaTemplate.send(topic, event.eventType(), event);

        future.whenComplete((result, exception) -> {
            if (exception != null) {
                handleFailure(event.eventType(), event, exception);
            } else {
                handleSuccess(result);
            }
        });
    }

    private void handleFailure(String key, NotificationEvent event, Throwable exception) {
        log.error("Failed to deliver message to topic {} with key {}. Error {}",
                topic, key, exception.getMessage(), exception);
    }

    private void handleSuccess(SendResult<String,NotificationEvent> result){
        log.info("Successfully sent message to topic {} partition {} offset {}",
                topic,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }
}
