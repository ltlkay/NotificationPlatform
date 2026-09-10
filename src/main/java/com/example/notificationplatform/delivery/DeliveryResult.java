package com.example.notificationplatform.delivery;

import com.example.notificationplatform.util.DeliveryStatus;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;

public record DeliveryResult(@NotNull DeliveryStatus status, String failureReason, int attemptCount) {
    public DeliveryResult {
        Objects.requireNonNull(status);
        if (attemptCount < 1) {
            throw new IllegalArgumentException("attemptCount should be no less than 1.");
        }
        if (status == DeliveryStatus.FAILED && failureReason == null) {
            throw new IllegalArgumentException("failureReason is required when status is FAILED");
        }
        if (status == DeliveryStatus.SUCCESS && failureReason != null) {
            throw new IllegalArgumentException("failureReason must be null when status is SUCCESS");
        }
    }

    public static DeliveryResult success(int attemptCount){

        return new DeliveryResult(DeliveryStatus.SUCCESS, null, attemptCount);
    }

    public static DeliveryResult failure(String reason, int attemptCount){
        return new DeliveryResult(DeliveryStatus.FAILED, Objects.requireNonNull(reason), attemptCount);
    }
}
