package com.example.notificationplatform.subscription;

import com.example.notificationplatform.util.ChannelType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "subscriptions", indexes = {
        @Index(name = "idx_event_type", columnList = "event_type"),
        @Index(name = "idx_channel_type", columnList = "channel")
})
@Getter
@NoArgsConstructor
public class Subscription {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", nullable = false)
    private ChannelType channel;

    @Column(nullable = false)
    private String target;

    @Column(nullable = false, updatable = false, columnDefinition = "TIMESTAMP WITH TIME ZONE")
    private Instant createdAt;

    public Subscription(String eventType, ChannelType channel, String target){
        this.eventType = Objects.requireNonNull(eventType);
        this.channel = Objects.requireNonNull(channel);
        this.target = Objects.requireNonNull(target);
        this.createdAt = Instant.now();
    }
}
