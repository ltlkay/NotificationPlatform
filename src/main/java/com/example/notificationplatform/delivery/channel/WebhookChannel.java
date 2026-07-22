package com.example.notificationplatform.delivery.channel;

import com.example.notificationplatform.delivery.DeliveryResult;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
@RequiredArgsConstructor
public class WebhookChannel implements NotificationChannel {

    private final RestClient client;

    @Override
    public DeliveryResult send(Subscription subscription, NotificationEvent event) {
         try {
            client.post()
                    .uri(subscription.getTarget())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(event)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException e) {
             return DeliveryResult.failure(e.getMessage());
         }
        return DeliveryResult.success();
    }
}
