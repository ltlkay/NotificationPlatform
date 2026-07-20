package com.example.notificationplatform.subscription;

import com.example.notificationplatform.util.ChannelType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@EntityListeners(AuditingEntityListener.class)
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

    @Column(name = "event_type")
    private String eventType;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel")
    private ChannelType channel;

    @Basic(optional = false)
    private String target;

    @CreatedDate
    @Column(nullable = false, updatable = false, columnDefinition = "TIMESTAMP WITH TIME ZONE")
    private Instant createdAt;

    public Subscription(String eventType, ChannelType channel, String target){
        this.eventType = eventType;
        this.channel = channel;
        this.target = Objects.requireNonNull(target);
        this.createdAt = Instant.now();
    }
}
