package com.example.notificationplatform.delivery;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface DeliveryLogRepository extends JpaRepository<DeliveryLog, UUID> {

    List<DeliveryLog> findBySubscriptionId(UUID subscriptionId);
}
