package com.example.notificationplatform.delivery.channel;

import com.example.notificationplatform.delivery.DeliveryResult;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;
import com.example.notificationplatform.util.WaitStrategy;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;

@Component
@RequiredArgsConstructor
public class WebhookChannel implements NotificationChannel {

    private final RestClient client;

    @Value("${notification.webhook.initial-delay-ms}")
    private long initDelay;

    @Value("${notification.webhook.multiplier}")
    private long multiplier;

    @Value("${notification.webhook.max-attempts}")
    private int maxAttempts;

    private final WaitStrategy waitStrategy;

    @Override
    public DeliveryResult send(Subscription subscription, NotificationEvent event) {
        long delay = initDelay;
        int attempt;
        for (attempt = 1; attempt <= maxAttempts; attempt++){
            try {
                client.post()
                        .uri(subscription.getTarget())
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(event)
                        .retrieve()
                        .toBodilessEntity();
                break;
            } catch (RestClientException e) {
                if (attempt == maxAttempts || e instanceof HttpClientErrorException) {
                    return DeliveryResult.failure(e.getMessage(), attempt);
                }
                if (e instanceof ResourceAccessException
                || e instanceof HttpServerErrorException) {
                    waitStrategy.stay(delay);
                    delay *= multiplier;
                } else {
                    return DeliveryResult.failure(e.getMessage(), attempt);
                }
            }
        }
        return DeliveryResult.success(attempt);
    }
}
