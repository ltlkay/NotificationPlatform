package com.example.notificationplatform.util;

import com.example.notificationplatform.delivery.DeadLetterEvent;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public final class KafkaTestData {
    public static final String EVENT_TYPE = "testEvent";
    public static final String TOPIC = "testTopic";
    public static final String DEAD_LETTER_TOPIC = "testDeadTopic";
    public static final String FAIL_REASON = "Dead line reason";
    public static final String TARGET = "testTarget";
    public static final int PART_NUM = 3;
    public static final long OFFSET = 42L;
    public static final String CALLBACK_ERROR_PREFIX = "Unhandled exception in Kafka send callback";
    public static final String BAD_METADATA_MSG = "... bad meta";

    private KafkaTestData(){}

    public static NotificationEvent createEvent() {
        return NotificationEvent.create(EVENT_TYPE, Map.of("data", "Test data"));
    }

    public static Subscription createSubscription(ChannelType channel) {
        Subscription sub = new Subscription(EVENT_TYPE, channel, TARGET);
        ReflectionTestUtils.setField(sub, "id", UUID.randomUUID());
        return sub;
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

    public static SendResult<String, DeadLetterEvent> createGoodSendResultDlq(DeadLetterEvent event) {
        ProducerRecord<String, DeadLetterEvent> record = new ProducerRecord<>(DEAD_LETTER_TOPIC, EVENT_TYPE, event);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(DEAD_LETTER_TOPIC, PART_NUM),
                OFFSET,
                0,
                System.currentTimeMillis(),
                0,
                10
        );
        return new SendResult<>(record, recordMetadata);
    }

    public static void stubSendNotification(KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                                            CompletableFuture<SendResult<String, NotificationEvent>> future,
                                            NotificationEvent event){
        when(kafkaTemplate.send(TOPIC, EVENT_TYPE, event)).thenReturn(future);
    }

    public static void stubSendDeadLetter(KafkaTemplate<String, DeadLetterEvent> kafkaTemplate,
                                          CompletableFuture<SendResult<String, DeadLetterEvent>> future) {
        when(kafkaTemplate.send(eq(DEAD_LETTER_TOPIC), eq(EVENT_TYPE), any(DeadLetterEvent.class))).thenReturn(future);
    }

    public static void stubSendSuccess(KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                                       NotificationEvent event) {
        when(kafkaTemplate.send(TOPIC, EVENT_TYPE, event))
                .thenReturn(CompletableFuture.completedFuture(createGoodSendResult(event)));
    }

    public static String successMessage(String topic) {
        return String.format("Successfully sent message to topic %s partition %d offset %d",
                topic, PART_NUM, OFFSET);
    }

    public static String failMessage(String topic) {
        return String.format("Failed to deliver message to topic %s with key %s",
                topic, EVENT_TYPE);
    }

    public static String failMessageWithReason(String topic, String reason) {
        return String.format("Failed to deliver message to topic %s with key %s. Error %s",
                topic, EVENT_TYPE, reason);
    }
}
