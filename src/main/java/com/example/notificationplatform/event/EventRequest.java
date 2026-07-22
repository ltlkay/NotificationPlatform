package com.example.notificationplatform.event;

import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record EventRequest(
        @NotBlank String eventType,
        Map<String, Object> payload) {}
