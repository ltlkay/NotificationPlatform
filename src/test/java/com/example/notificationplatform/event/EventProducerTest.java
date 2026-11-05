package com.example.notificationplatform.event;

import ch.qos.logback.classic.Level;
import com.example.notificationplatform.util.KafkaTestData;
import com.example.notificationplatform.util.TestLogAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static com.example.notificationplatform.util.KafkaTestData.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventProducerTest {
    @Mock
    private KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private TestLogAppender testLogAppender;

    @InjectMocks
    private EventProducer eventProducer;

    private static final String RT_EXCEPTION_MSG = "Boom test";

    @BeforeEach
    void setUp() {
        eventProducer.setTopic(TOPIC);
        testLogAppender = TestLogAppender.attachTo(EventProducer.class);
    }

    @AfterEach
    void tearDown() {
        testLogAppender.close();
    }

    @Test
    void send_forwardsEventTypeAsKey() {
        NotificationEvent testEvent = createEvent();
        stubSendSuccess(kafkaTemplate, testEvent);

        eventProducer.send(testEvent);

        verify(kafkaTemplate).send(TOPIC, EVENT_TYPE, testEvent);
        verifyNoMoreInteractions(kafkaTemplate);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void send_whenCallbackSuccess_logsSuccess() {
        NotificationEvent testEvent = createEvent();
        stubSendSuccess(kafkaTemplate, testEvent);

        eventProducer.send(testEvent);

        testLogAppender.assertLogged(Level.INFO, successMessage(TOPIC));
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void send_whenCallbackFails_logsFailure() {
        NotificationEvent testEvent = createEvent();
        stubSendNotification(kafkaTemplate,
                CompletableFuture.failedFuture(new RuntimeException(RT_EXCEPTION_MSG)),
                testEvent);

        eventProducer.send(testEvent);

        assertThat(testLogAppender.eventsAt(Level.ERROR)).hasSize(1);
        assertThat(testLogAppender.eventsAt(Level.ERROR)).anySatisfy(event -> {
            assertThat(event.getFormattedMessage()).startsWith(failMessage(TOPIC));
            assertThat(event.getThrowableProxy()).isNotNull();
            assertThat(event.getThrowableProxy().getMessage()).isEqualTo(RT_EXCEPTION_MSG);
        });
        testLogAppender.assertNotLogged(Level.INFO, successMessage(TOPIC));
    }

    @Test
    void send_whenCallbackThrows_logsUnhandledError() {
        NotificationEvent testEvent = createEvent();
        SendResult<String, NotificationEvent> mockResult = mock();
        stubSendNotification(kafkaTemplate,
                CompletableFuture.completedFuture(mockResult),
                testEvent);
        when(mockResult.getRecordMetadata()).thenThrow(new RuntimeException(BAD_METADATA_MSG));

        eventProducer.send(testEvent);

        assertThat(testLogAppender.eventsAt(Level.ERROR)).hasSize(1);
        assertThat(testLogAppender.eventsAt(Level.ERROR)).anySatisfy(event -> {
            assertThat(event.getFormattedMessage()).startsWith(CALLBACK_ERROR_PREFIX);
            assertThat(event.getThrowableProxy()).isNotNull();
            assertThat(event.getThrowableProxy().getMessage()).isEqualTo(BAD_METADATA_MSG);
        });
        testLogAppender.assertNotLogged(Level.INFO, successMessage(TOPIC));
    }
}

