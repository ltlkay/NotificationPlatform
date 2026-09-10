package com.example.notificationplatform.delivery;

import com.example.notificationplatform.event.NotificationEvent;


import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record DeadLetterEvent(UUID subscriptionId,
                              NotificationEvent event,
                              String failureReason,
                              int attemptCount,
                              Instant deadLetteredAt) {
    public DeadLetterEvent {
        Objects.requireNonNull(subscriptionId);
        Objects.requireNonNull(event);
        Objects.requireNonNull(deadLetteredAt);
        if (failureReason == null || failureReason.isBlank()) {
            throw new IllegalArgumentException("Failure reason is required.");
        }
    }

    public static DeadLetterEvent of(UUID subscriptionId, NotificationEvent event,
                                     String failureReason, int attemptCount) {
        return new DeadLetterEvent(subscriptionId, event, failureReason, attemptCount, Instant.now());
    }
}
