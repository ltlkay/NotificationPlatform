package com.example.notificationplatform.subscription;

import com.example.notificationplatform.util.ChannelType;

import java.time.Instant;
import java.util.UUID;

public record SubscriptionResponse(UUID id, String eventType, ChannelType channel,
                                   String target, Instant createdAt) {
    public static SubscriptionResponse from(Subscription s) {
        return new SubscriptionResponse(s.getId(), s.getEventType(), s.getChannel(),
                s.getTarget(), s.getCreatedAt());
    }
}