package com.example.notificationplatform.delivery;

import com.example.notificationplatform.util.DeliveryStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@NoArgsConstructor
@Getter
@Table(name = "delivery_logs", indexes =
    @Index(name = "idx_subscription_id", columnList = "subscription_id"))
public class DeliveryLog {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name =  "subscription_id", nullable = false)
    private UUID subscriptionId;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false)
    private DeliveryStatus status;

    @Column(nullable = false)
    private Instant attemptedAt;

    private String failureReason;

    private DeliveryLog(UUID subscriptionId, UUID eventId,
                        DeliveryStatus status, String failureReason) {
        this.subscriptionId = Objects.requireNonNull(subscriptionId);
        this.eventId = Objects.requireNonNull(eventId);
        this.status = status;
        this.failureReason = failureReason;
        this.attemptedAt = Instant.now();
    }

    public static DeliveryLog onSuccess(UUID subscriptionId, UUID eventId) {
        return new DeliveryLog(subscriptionId, eventId, DeliveryStatus.SUCCESS, null);
    }

    public static DeliveryLog onFailure(UUID subscriptionId, UUID eventId, String reason) {
        return new DeliveryLog(subscriptionId, eventId, DeliveryStatus.FAILED, Objects.requireNonNull(reason));
    }

}
