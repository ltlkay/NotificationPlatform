package com.example.notificationplatform.delivery;

import ch.qos.logback.classic.Level;
import com.example.notificationplatform.delivery.channel.NotificationChannel;
import com.example.notificationplatform.event.NotificationEvent;
import com.example.notificationplatform.subscription.Subscription;
import com.example.notificationplatform.subscription.SubscriptionRepository;
import com.example.notificationplatform.util.ChannelType;
import com.example.notificationplatform.util.DeliveryStatus;
import com.example.notificationplatform.util.TestLogAppender;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.example.notificationplatform.util.KafkaTestData.BAD_METADATA_MSG;
import static com.example.notificationplatform.util.KafkaTestData.CALLBACK_ERROR_PREFIX;
import static com.example.notificationplatform.util.KafkaTestData.DEAD_LETTER_TOPIC;
import static com.example.notificationplatform.util.KafkaTestData.EVENT_TYPE;
import static com.example.notificationplatform.util.KafkaTestData.FAIL_REASON;
import static com.example.notificationplatform.util.KafkaTestData.createEvent;
import static com.example.notificationplatform.util.KafkaTestData.createGoodSendResultDlq;
import static com.example.notificationplatform.util.KafkaTestData.createSubscription;
import static com.example.notificationplatform.util.KafkaTestData.failMessage;
import static com.example.notificationplatform.util.KafkaTestData.stubSendDeadLetter;
import static com.example.notificationplatform.util.KafkaTestData.successMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationConsumerTest {
    private static final String BOOM = "Boom";
    private static final String BADOOM = "Badoom";
    private static final String STALE_CONNECTION = "Stale connection";
    private static final String BAD_INTERNET_1 = "Bad Internet 1";
    private static final String BAD_INTERNET_2 = "Bad Internet 2";
    private static final String LAGGY_NATURE = "Laggy Nature";
    private static final String BAD_DAY = "Bad day";
    private static final String SERIAL_KILL = "Serial kill";
    private static final String DB_DOWN = "DB down";
    private static final String SEND_FAILURE = "Send failed";

    @Mock
    private KafkaTemplate<String, DeadLetterEvent> kafkaTemplate;
    @Mock
    private SubscriptionRepository subscriptionRepository;
    @Mock
    private DeliveryLogRepository deliveryLogRepository;
    @Mock
    private NotificationChannel emailChannel;
    @Mock
    private NotificationChannel webhookChannel;
    @Captor
    private ArgumentCaptor<DeliveryLog> deliveryLogCaptor;
    @Captor
    private ArgumentCaptor<DeadLetterEvent> dlEventCaptor;

    private TestLogAppender testLogAppender;
    private NotificationConsumer consumer;

    @BeforeEach
    void setUp() {
        createConsumer();
        consumer.setDeadLetterTopic(DEAD_LETTER_TOPIC);
        testLogAppender = TestLogAppender.attachTo(NotificationConsumer.class);
    }

    @AfterEach
    void tearDown() {
        testLogAppender.close();
    }

    @Test
    void consume_whenEmailSuccess_logsSuccessNoDlq() {
        NotificationEvent event = createEvent();
        Subscription emailSub = createSubscription(ChannelType.EMAIL);
        givenSubscriptions(emailSub);
        when(emailChannel.send(emailSub, event)).thenReturn(DeliveryResult.success(1));

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        assertDeliveryLogs(tuple(emailSub.getId(), DeliveryStatus.SUCCESS, null, 1));
        verifyNoInteractions(webhookChannel);
        verifyNoInteractions(kafkaTemplate);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenWebhookSuccess_logsSuccessNoDlq() {
        NotificationEvent event = createEvent();
        Subscription webhookSub = createSubscription(ChannelType.WEBHOOK);
        givenSubscriptions(webhookSub);
        when(webhookChannel.send(webhookSub, event)).thenReturn(DeliveryResult.success(2));

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        assertDeliveryLogs(tuple(webhookSub.getId(), DeliveryStatus.SUCCESS, null, 2));
        verifyNoInteractions(emailChannel);
        verifyNoInteractions(kafkaTemplate);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenNoSubs_doesNothing() {
        NotificationEvent event = createEvent();
        givenSubscriptions();

        consumer.consumeEvent(event);

        verifyNoInteractions(emailChannel);
        verifyNoInteractions(webhookChannel);
        verifyNoInteractions(deliveryLogRepository);
        verifyNoInteractions(kafkaTemplate);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenWebhookFail_sendsToDlq() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        givenSubscriptions(webSub);
        when(webhookChannel.send(webSub, event))
                .thenReturn(DeliveryResult.failure(FAIL_REASON, 5));
        stubDlqNeverCompletes();

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(tuple(webSub.getId(), DeliveryStatus.FAILED, FAIL_REASON, 5));
        assertDeadLettered(tuple(webSub.getId(), event, FAIL_REASON, 5));
        verifyNoInteractions(emailChannel);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenEmailFail_noDlq() {
        NotificationEvent event = createEvent();
        Subscription emailSub = createSubscription(ChannelType.EMAIL);
        givenSubscriptions(emailSub);
        when(emailChannel.send(emailSub, event))
                .thenReturn(DeliveryResult.failure(FAIL_REASON, 3));

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        assertDeliveryLogs(tuple(emailSub.getId(), DeliveryStatus.FAILED, FAIL_REASON, 3));
        verifyNoInteractions(webhookChannel);
        verifyNoInteractions(kafkaTemplate);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenWebhookThrows_sendsDlqWithMessage() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        givenSubscriptions(webSub);
        when(webhookChannel.send(webSub, event)).thenThrow(new RuntimeException(BOOM));
        stubDlqNeverCompletes();

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(tuple(webSub.getId(), DeliveryStatus.FAILED, BOOM, 1));
        assertDeadLettered(tuple(webSub.getId(), event, BOOM, 1));
        verifyNoInteractions(emailChannel);
        testLogAppender.assertNoErrorLogged();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void consume_whenWebhookThrowsNoUsableMessage_sendsDlqWithClassName(String message) {
        String className = IllegalArgumentException.class.getSimpleName();
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        givenSubscriptions(webSub);
        when(webhookChannel.send(webSub, event)).thenThrow(new IllegalArgumentException(message));
        stubDlqNeverCompletes();

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(tuple(webSub.getId(), DeliveryStatus.FAILED, className, 1));
        assertDeadLettered(tuple(webSub.getId(), event, className, 1));
        verifyNoInteractions(emailChannel);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenEmailThrows_noDlq() {
        NotificationEvent event = createEvent();
        Subscription emailSub = createSubscription(ChannelType.EMAIL);
        givenSubscriptions(emailSub);
        when(emailChannel.send(emailSub, event)).thenThrow(new RuntimeException(BOOM));

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        assertDeliveryLogs(tuple(emailSub.getId(), DeliveryStatus.FAILED, BOOM, 1));
        verifyNoInteractions(kafkaTemplate);
        verifyNoInteractions(webhookChannel);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenMultiSubsOneThrows_oneFailWithDlqOneSuccess() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        Subscription emailSub = createSubscription(ChannelType.EMAIL);
        givenSubscriptions(webSub, emailSub);
        when(webhookChannel.send(webSub, event)).thenThrow(new RuntimeException(BADOOM));
        when(emailChannel.send(emailSub, event)).thenReturn(DeliveryResult.success(1));
        stubDlqNeverCompletes();

        consumer.consumeEvent(event);

        verifyLogsSaved(2);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(
                tuple(webSub.getId(), DeliveryStatus.FAILED, BADOOM, 1),
                tuple(emailSub.getId(), DeliveryStatus.SUCCESS, null, 1));
        assertDeadLettered(tuple(webSub.getId(), event, BADOOM, 1));
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenMultiSubsOneFails_oneFailNoDlqOneSuccess() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        Subscription emailSub = createSubscription(ChannelType.EMAIL);
        givenSubscriptions(emailSub, webSub);
        when(webhookChannel.send(webSub, event)).thenReturn(DeliveryResult.success(1));
        when(emailChannel.send(emailSub, event))
                .thenReturn(DeliveryResult.failure(STALE_CONNECTION, 1));

        consumer.consumeEvent(event);

        verifyLogsSaved(2);
        assertDeliveryLogs(
                tuple(emailSub.getId(), DeliveryStatus.FAILED, STALE_CONNECTION, 1),
                tuple(webSub.getId(), DeliveryStatus.SUCCESS, null, 1));
        verifyNoInteractions(kafkaTemplate);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenMultiSubsFail_distinctDlqAndLog() {
        NotificationEvent event = createEvent();
        Subscription webSub1 = createSubscription(ChannelType.WEBHOOK);
        Subscription webSub2 = createSubscription(ChannelType.WEBHOOK);
        givenSubscriptions(webSub1, webSub2);
        when(webhookChannel.send(webSub1, event))
                .thenReturn(DeliveryResult.failure(BAD_INTERNET_1, 1));
        when(webhookChannel.send(webSub2, event))
                .thenReturn(DeliveryResult.failure(BAD_INTERNET_2, 3));
        stubDlqNeverCompletes();

        consumer.consumeEvent(event);

        verifyLogsSaved(2);
        verifyDeadLettersSent(2);
        assertDeliveryLogs(
                tuple(webSub1.getId(), DeliveryStatus.FAILED, BAD_INTERNET_1, 1),
                tuple(webSub2.getId(), DeliveryStatus.FAILED, BAD_INTERNET_2, 3));
        assertDeadLettered(
                tuple(webSub1.getId(), event, BAD_INTERNET_1, 1),
                tuple(webSub2.getId(), event, BAD_INTERNET_2, 3));
        verifyNoInteractions(emailChannel);
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenDlqSendSuccess_logsInfo() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        DeadLetterEvent deadLetterEvent = DeadLetterEvent.of(webSub.getId(), event, LAGGY_NATURE, 2);
        givenSubscriptions(webSub);
        when(webhookChannel.send(webSub, event))
                .thenReturn(DeliveryResult.failure(LAGGY_NATURE, 2));
        stubSendDeadLetter(kafkaTemplate,
                CompletableFuture.completedFuture(createGoodSendResultDlq(deadLetterEvent)));

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(tuple(webSub.getId(), DeliveryStatus.FAILED, LAGGY_NATURE, 2));
        testLogAppender.assertLogged(Level.INFO, successMessage(DEAD_LETTER_TOPIC));
        testLogAppender.assertNoErrorLogged();
    }

    @Test
    void consume_whenDlqSendFail_logsError() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        givenSubscriptions(webSub);
        when(webhookChannel.send(webSub, event))
                .thenReturn(DeliveryResult.failure(LAGGY_NATURE, 2));
        stubSendDeadLetter(kafkaTemplate,
                CompletableFuture.failedFuture(new RuntimeException(SEND_FAILURE)));

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(tuple(webSub.getId(), DeliveryStatus.FAILED, LAGGY_NATURE, 2));
        assertDeadLettered(tuple(webSub.getId(), event, LAGGY_NATURE, 2));
        assertThat(testLogAppender.eventsAt(Level.ERROR)).singleElement().satisfies(loggingEvent -> {
            assertThat(loggingEvent.getFormattedMessage()).startsWith(failMessage(DEAD_LETTER_TOPIC));
            assertThat(loggingEvent.getThrowableProxy()).isNotNull();
            assertThat(loggingEvent.getThrowableProxy().getMessage()).isEqualTo(SEND_FAILURE);
        });
        testLogAppender.assertNotLogged(Level.INFO, successMessage(DEAD_LETTER_TOPIC));
    }

    @Test
    void consume_whenDlqCallbackThrows_stillSavesLogAndLogsError() {
        NotificationEvent event = createEvent();
        // Mock (not a real SendResult) so that getRecordMetadata() can throw inside the callback.
        SendResult<String, DeadLetterEvent> mockResult = mock();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        givenSubscriptions(webSub);
        when(webhookChannel.send(webSub, event))
                .thenReturn(DeliveryResult.failure(LAGGY_NATURE, 2));
        stubSendDeadLetter(kafkaTemplate, CompletableFuture.completedFuture(mockResult));
        when(mockResult.getRecordMetadata()).thenThrow(new RuntimeException(BAD_METADATA_MSG));

        consumer.consumeEvent(event);

        verifyLogsSaved(1);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(tuple(webSub.getId(), DeliveryStatus.FAILED, LAGGY_NATURE, 2));
        assertThat(testLogAppender.eventsAt(Level.ERROR)).singleElement().satisfies(e -> {
            assertThat(e.getFormattedMessage()).startsWith(CALLBACK_ERROR_PREFIX);
            assertThat(e.getThrowableProxy()).isNotNull();
            assertThat(e.getThrowableProxy().getMessage()).isEqualTo(BAD_METADATA_MSG);
        });
        testLogAppender.assertNotLogged(Level.INFO, successMessage(DEAD_LETTER_TOPIC));
    }

    @Test
    void consume_whenDlqSendThrowsSynchronously_logsErrorAndSavesRest() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        Subscription emailSub = createSubscription(ChannelType.EMAIL);
        givenSubscriptions(webSub, emailSub);
        when(webhookChannel.send(webSub, event))
                .thenReturn(DeliveryResult.failure(BAD_DAY, 2));
        when(emailChannel.send(emailSub, event))
                .thenReturn(DeliveryResult.success(1));
        when(kafkaTemplate.send(eq(DEAD_LETTER_TOPIC), eq(EVENT_TYPE), any(DeadLetterEvent.class)))
                .thenThrow(new RuntimeException(SERIAL_KILL));

        consumer.consumeEvent(event);

        verifyLogsSaved(2);
        verifyDeadLettersSent(1);
        assertDeliveryLogs(
                tuple(webSub.getId(), DeliveryStatus.FAILED, BAD_DAY, 2),
                tuple(emailSub.getId(), DeliveryStatus.SUCCESS, null, 1));
        assertThat(testLogAppender.eventsAt(Level.ERROR)).singleElement().satisfies(e -> {
            assertThat(e.getFormattedMessage()).startsWith(failMessage(DEAD_LETTER_TOPIC));
            assertThat(e.getThrowableProxy()).isNotNull();
            assertThat(e.getThrowableProxy().getMessage()).isEqualTo(SERIAL_KILL);
        });
    }

    @Test
    @DisplayName("Deliberate: save failure propagates so Kafka redelivers (duplicates tracked in idempotency ticket)")
    void consume_whenSaveThrows_propagatesAndStops() {
        NotificationEvent event = createEvent();
        Subscription webSub = createSubscription(ChannelType.WEBHOOK);
        Subscription emailSub = createSubscription(ChannelType.EMAIL);
        givenSubscriptions(emailSub, webSub);
        when(emailChannel.send(emailSub, event))
                .thenReturn(DeliveryResult.success(1));
        RuntimeException ex = new RuntimeException(DB_DOWN);
        when(deliveryLogRepository.save(any())).thenThrow(ex);

        assertThatThrownBy(() -> consumer.consumeEvent(event)).isSameAs(ex);

        verify(deliveryLogRepository).save(any());
        verifyNoInteractions(webhookChannel);
        verifyNoInteractions(kafkaTemplate);
    }

    private void createConsumer() {
        this.consumer = new NotificationConsumer(subscriptionRepository,
                deliveryLogRepository,
                emailChannel,
                webhookChannel,
                kafkaTemplate
        );
    }

    private void givenSubscriptions(Subscription... subs) {
        when(subscriptionRepository.findByEventType(EVENT_TYPE)).thenReturn(List.of(subs));
    }

    /** DLQ send that never completes: no success or failure callback fires. */
    private void stubDlqNeverCompletes() {
        stubSendDeadLetter(kafkaTemplate, new CompletableFuture<>());
    }

    private void verifyLogsSaved(int times) {
        verify(deliveryLogRepository, times(times)).save(deliveryLogCaptor.capture());
    }

    private void verifyDeadLettersSent(int times) {
        verify(kafkaTemplate, times(times))
                .send(eq(DEAD_LETTER_TOPIC), eq(EVENT_TYPE), dlEventCaptor.capture());
    }

    private void assertDeliveryLogs(Tuple... expected) {
        assertThat(deliveryLogCaptor.getAllValues()).extracting(DeliveryLog::getSubscriptionId,
                        DeliveryLog::getStatus,
                        DeliveryLog::getFailureReason,
                        DeliveryLog::getAttemptCount)
                .containsExactlyInAnyOrder(expected);
    }

    private void assertDeadLettered(Tuple... expected) {
        assertThat(dlEventCaptor.getAllValues()).extracting(DeadLetterEvent::subscriptionId,
                        DeadLetterEvent::event,
                        DeadLetterEvent::failureReason,
                        DeadLetterEvent::attemptCount)
                .containsExactlyInAnyOrder(expected);
    }
}
