package com.example.notificationplatform.subscription;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.List;
import java.util.UUID;

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
        URI uri = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}").buildAndExpand(saved.getId()).toUri();
        return ResponseEntity.created(uri)
                .body(SubscriptionResponse.from(saved));
    }

    @GetMapping("/subscriptions")
    public List<SubscriptionResponse> listSubscriptions(@RequestParam(required = false) String eventType) {
        List<Subscription> result = (eventType != null) ? repository.findByEventType(eventType)
                : repository.findAll();
        return result.stream().map(SubscriptionResponse::from).toList();
    }

    @GetMapping("/subscriptions/{id}")
    public SubscriptionResponse getSubscriptionById(@PathVariable UUID id) {
        return repository.findById(id).map(SubscriptionResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "No subscription with id: " + id));
    }

    @DeleteMapping("/subscriptions/{id}")
    public ResponseEntity<Void> deleteSubscriptionById(@PathVariable UUID id) {
        repository.deleteById(id);
        return ResponseEntity.noContent().build();
    }
}
