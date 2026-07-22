package com.example.notificationplatform.delivery;

import com.example.notificationplatform.delivery.channel.EmailChannel;
import com.example.notificationplatform.delivery.channel.NotificationChannel;
import com.example.notificationplatform.delivery.channel.WebhookChannel;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;
import com.example.notificationplatform.subscription.SubscriptionRepository;
import com.example.notificationplatform.util.ChannelType;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class NotificationConsumer {

    private final SubscriptionRepository subscriptionRepository;
    private final DeliveryLogRepository deliveryLogRepository;
    private final NotificationChannel emailChannel;
    private final NotificationChannel webhookChannel;

    @KafkaListener(topics = "${spring.kafka.template.default-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvent(NotificationEvent event) {
        List<Subscription> matchingSubscriptions = subscriptionRepository.findByEventType(event.eventType());
        for (Subscription sub : matchingSubscriptions){
            DeliveryResult result;
            try {
                result = resolveChannel(sub.getChannel()).send(sub, event);
            } catch (Exception e){
                result = DeliveryResult.failure(e.getMessage());
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
            case SUCCESS -> DeliveryLog.onSuccess(subscription.getId(),event.eventId());
            case FAILED -> DeliveryLog.onFailure(subscription.getId(),event.eventId(),result.failureReason());
        };
    }
}
