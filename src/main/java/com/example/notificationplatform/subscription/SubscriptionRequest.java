package com.example.notificationplatform.subscription;

import com.example.notificationplatform.util.ChannelType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record SubscriptionRequest(
        @NotBlank String eventType,
        @NotNull ChannelType channel,
        @NotBlank String target) {}

