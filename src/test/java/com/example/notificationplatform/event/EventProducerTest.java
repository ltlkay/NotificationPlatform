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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import static com.example.notificationplatform.util.KafkaTestData.createEvent;
import static com.example.notificationplatform.util.KafkaTestData.stubSend;
import static com.example.notificationplatform.util.KafkaTestData.stubSendSuccess;
import static com.example.notificationplatform.util.KafkaTestData.TOPIC;
import static com.example.notificationplatform.util.KafkaTestData.EVENT_TYPE;
import static com.example.notificationplatform.util.KafkaTestData.PART_NUM;
import static com.example.notificationplatform.util.KafkaTestData.OFFSET;

@ExtendWith(MockitoExtension.class)
class EventProducerTest {
    @Mock
    private KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private TestLogAppender testLogAppender;

    @InjectMocks
    private EventProducer eventProducer;

    private static final String SUCCESS_MSG =
            String.format("Successfully sent message to topic %s partition %d offset %d",
                    TOPIC, PART_NUM, OFFSET);
    private static final String FAIL_MSG = String.format("Failed to deliver message to topic %s with key %s",
            TOPIC, EVENT_TYPE);
    private static final String RT_EXCEPTION_MSG = "Boom test";
    private static final String THROW_EXCEPTION_MSG = "... bad meta";
    private static final String THROW_MSG = "Unhandled exception in Kafka send callback";

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

        testLogAppender.assertLogged(Level.INFO, SUCCESS_MSG);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void send_whenCallbackFails_logsFailure() {
        NotificationEvent testEvent = createEvent();
        stubSend(kafkaTemplate,
                CompletableFuture.failedFuture(new RuntimeException(RT_EXCEPTION_MSG)),
                testEvent);

        eventProducer.send(testEvent);

        assertThat(testLogAppender.eventsAt(Level.ERROR)).anySatisfy(event -> {
            assertThat(event.getFormattedMessage()).startsWith(FAIL_MSG);
            assertThat(event.getThrowableProxy()).isNotNull();
            assertThat(event.getThrowableProxy().getMessage()).isEqualTo(RT_EXCEPTION_MSG);
        });
        testLogAppender.assertNotLogged(Level.INFO, SUCCESS_MSG);
    }

    @Test
    void send_whenCallbackThrows_logsUnhandledError() {
        NotificationEvent testEvent = createEvent();
        SendResult<String, NotificationEvent> mockResult = mock();
        stubSend(kafkaTemplate,
                CompletableFuture.completedFuture(mockResult),
                testEvent);
        when(mockResult.getRecordMetadata()).thenThrow(new RuntimeException(THROW_EXCEPTION_MSG));

        eventProducer.send(testEvent);

        assertThat(testLogAppender.eventsAt(Level.ERROR)).anySatisfy(event -> {
            assertThat(event.getFormattedMessage()).startsWith(THROW_MSG);
            assertThat(event.getThrowableProxy()).isNotNull();
            assertThat(event.getThrowableProxy().getCause()).isNotNull();
            assertThat(event.getThrowableProxy().getCause().getMessage()).isEqualTo(THROW_EXCEPTION_MSG);
        });
        testLogAppender.assertNotLogged(Level.INFO, SUCCESS_MSG);
    }
}

