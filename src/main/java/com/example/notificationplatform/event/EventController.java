package com.example.notificationplatform.event;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class EventController {

    private final EventProducer producer;

    public EventController(EventProducer producer) {this.producer = producer;}

    @PostMapping("/events")
    public ResponseEntity<UUID> publishEvent(@RequestBody @Valid EventRequest request){
        NotificationEvent event = NotificationEvent.create(request.eventType(), request.payload());
        producer.send(event);
        return ResponseEntity.accepted().body(event.eventId());
    }
}
