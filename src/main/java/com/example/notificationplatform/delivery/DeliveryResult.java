package com.example.notificationplatform.delivery;

import com.example.notificationplatform.util.DeliveryStatus;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;

public record DeliveryResult(@NotNull DeliveryStatus status, String failureReason) {
    public DeliveryResult {
        Objects.requireNonNull(status);
        if (status == DeliveryStatus.FAILED && failureReason == null) {
            throw new IllegalArgumentException("failureReason is required when status is FAILED");
        }
        if (status == DeliveryStatus.SUCCESS && failureReason != null) {
            throw new IllegalArgumentException("failureReason must be null when status is SUCCESS");
        }
    }

    public static DeliveryResult success(){
        return new DeliveryResult(DeliveryStatus.SUCCESS, null);
    }

    public static DeliveryResult failure(String reason){
        return new DeliveryResult(DeliveryStatus.FAILED, Objects.requireNonNull(reason));
    }
}
