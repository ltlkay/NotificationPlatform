package com.example.notificationplatform.util;

import com.example.notificationplatform.event.NotificationEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.when;

public final class KafkaTestData {
    public static final String EVENT_TYPE = "testEvent";
    public static final String TOPIC = "testTopic";
    public static final int PART_NUM = 3;
    public static final long OFFSET = 42L;

    private KafkaTestData(){}

    public static NotificationEvent createEvent() {
        return NotificationEvent.create(EVENT_TYPE, Map.of("data", "Test data"));
    }

    public static SendResult<String, NotificationEvent> createGoodSendResult(NotificationEvent event) {
        ProducerRecord<String, NotificationEvent> record = new ProducerRecord<>(TOPIC, EVENT_TYPE, event);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(TOPIC, PART_NUM),
                OFFSET,
                0,
                System.currentTimeMillis(),
                0,
                10
        );
        return new SendResult<>(record, recordMetadata);
    }

    public static void stubSend(KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                                CompletableFuture<SendResult<String, NotificationEvent>> future,
                                NotificationEvent event){
        when(kafkaTemplate.send(TOPIC, EVENT_TYPE, event)).thenReturn(future);
    }

    public static void stubSendSuccess(KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                                       NotificationEvent event) {
        when(kafkaTemplate.send(TOPIC, EVENT_TYPE, event))
                .thenReturn(CompletableFuture.completedFuture(createGoodSendResult(event)));
    }
}
