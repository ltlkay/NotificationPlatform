package com.example.notificationplatform.event;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

import static com.example.notificationplatform.util.FailureReasons.reasonOf;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {
    private final KafkaTemplate<String,NotificationEvent> kafkaTemplate;

    @Value("${spring.kafka.template.default-topic}")
    private String topic;

    public void send(NotificationEvent event) {
        CompletableFuture<SendResult<String, NotificationEvent>> future = kafkaTemplate.send(topic, event.eventType(), event);

        future.handle((result, ex) -> {
            try {
                if (ex != null) handleFailure(event.eventType(), ex);
                else handleSuccess(result);
            } catch (Exception e) {
                log.error("Unhandled exception in Kafka send callback", e);
            }
            return null;
        });
    }

    private void handleFailure(String key, Throwable exception) {
        log.error("Failed to deliver message to topic {} with key {}. Error {}",
                topic, key, reasonOf(exception), exception);
    }

    private void handleSuccess(SendResult<String,NotificationEvent> result){
        log.info("Successfully sent message to topic {} partition {} offset {}",
                topic,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }

    void setTopic(String topic) {
        this.topic = topic;
    }
}
