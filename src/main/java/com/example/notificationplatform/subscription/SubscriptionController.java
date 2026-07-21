package com.example.notificationplatform.subscription;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
class SubscriptionController {

    private final SubscriptionRepository repository;

    SubscriptionController(SubscriptionRepository repository) {
        this.repository = repository;
    }

    @PostMapping("/subscriptions")
    public ResponseEntity<SubscriptionResponse> createSubscription(@RequestBody @Valid SubscriptionRequest request) {
        Subscription entity = new Subscription(request.eventType(), request.channel(), request.target());
        Subscription saved = repository.save(entity);
        return ResponseEntity.status(HttpStatus.CREATED).body(SubscriptionResponse.from(saved));
    }

    @GetMapping("/subscriptions")
    public List<SubscriptionResponse> listSubscriptions(@RequestParam(required = false) String eventType) {
        List<Subscription> result = (eventType != null)? repository.findByEventType(eventType)
                : repository.findAll();
        return result.stream().map(SubscriptionResponse::from).toList();
    }
}
