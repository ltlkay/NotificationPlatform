package com.example.notificationplatform.delivery.channel;

import com.example.notificationplatform.delivery.DeliveryResult;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;

public interface NotificationChannel {
    DeliveryResult send(Subscription subscription, NotificationEvent event);
}
