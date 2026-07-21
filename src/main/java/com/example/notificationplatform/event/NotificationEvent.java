package com.example.notificationplatform.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record NotificationEvent(@NotNull UUID eventId,
                                @NotBlank String eventType,
                                Map<String, Object> payload,
                                @NotNull Instant occurredAt) {
    public NotificationEvent {
        payload = (payload == null)? Map.of() : Map.copyOf(payload);
    }

    public static NotificationEvent create(String eventType, Map<String, Object> payload) {
        return new NotificationEvent(UUID.randomUUID(), eventType, payload, Instant.now());
    }
}
