package com.example.notificationplatform.delivery.channel;

import com.example.notificationplatform.delivery.DeliveryResult;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
public class EmailChannel implements NotificationChannel{
    @Value("${notification.email.failure-rate:0.0}")
    private double failureRate;

    public DeliveryResult send(Subscription subscription, NotificationEvent event) {

        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            return DeliveryResult.failure("simulated email failure");
        }
        log.info("email sent to {}: {} - {}", subscription.getTarget(), event.eventType(), event.payload());
        return DeliveryResult.success();
    }
}
