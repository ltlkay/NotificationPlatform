package com.example.notificationplatform.delivery;

import com.example.notificationplatform.delivery.channel.NotificationChannel;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;
import com.example.notificationplatform.subscription.SubscriptionRepository;
import com.example.notificationplatform.util.ChannelType;
import com.example.notificationplatform.util.DeliveryStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final SubscriptionRepository subscriptionRepository;
    private final DeliveryLogRepository deliveryLogRepository;
    private final NotificationChannel emailChannel;
    private final NotificationChannel webhookChannel;
    private final KafkaTemplate<String,DeadLetterEvent> kafkaTemplate;

    @Value("${spring.kafka.template.dlq-topic}")
    private String deadLetterTopic;

    @KafkaListener(topics = "${spring.kafka.template.default-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvent(NotificationEvent event) {
        List<Subscription> matchingSubscriptions = subscriptionRepository.findByEventType(event.eventType());
        for (Subscription sub : matchingSubscriptions){
            DeliveryResult result;
            try {
                result = resolveChannel(sub.getChannel()).send(sub, event);
            } catch (Exception e){
                result = DeliveryResult.failure(e.getMessage(),1);
            }
            if (sub.getChannel() == ChannelType.WEBHOOK && result.status() == DeliveryStatus.FAILED) {
                DeadLetterEvent dEvent = DeadLetterEvent.of(sub.getId(),event,result.failureReason(),result.attemptCount());
                CompletableFuture<SendResult<String, DeadLetterEvent>> future = kafkaTemplate.send(deadLetterTopic,
                        event.eventType(),
                        dEvent);
                future.whenComplete((dResult, exception) -> {
                    if (exception != null) {
                        handleFailure(event.eventType(), exception);
                    } else {
                        handleSuccess(dResult);
                    }
                });
            }
            deliveryLogRepository.save(toDeliveryLog(sub,event,result));
        }
    }

    private NotificationChannel resolveChannel(ChannelType channel){
        return switch (channel) {
            case EMAIL -> emailChannel;
            case WEBHOOK -> webhookChannel;
        };
    }

    private DeliveryLog toDeliveryLog(Subscription subscription, NotificationEvent event, DeliveryResult result){
        return switch (result.status()) {
            case SUCCESS -> DeliveryLog.onSuccess(subscription.getId(),event.eventId(), result.attemptCount());
            case FAILED -> DeliveryLog.onFailure(subscription.getId(),event.eventId(),result.failureReason(), result.attemptCount());
        };
    }

    private void handleFailure(String key, Throwable exception) {
        log.error("Failed to deliver message to topic {} with key {}. Error {}",
                deadLetterTopic, key, exception.getMessage(), exception);
    }

    private void handleSuccess(SendResult<String,DeadLetterEvent> result){
        log.info("Successfully sent message to topic {} partition {} offset {}",
                deadLetterTopic,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }
}
