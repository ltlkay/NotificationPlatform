package com.example.notificationplatform.subscription;

import com.example.notificationplatform.util.ChannelType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@WebMvcTest(SubscriptionController.class)
public class SubscriptionControllerTest {
    private static final String EVENT_TYPE = "Test_event";
    private static final String SUBSCRIPTION_BY_ID = "/subscriptions/{id}";
    private static final String SUBSCRIPTION = "/subscriptions";
    @MockitoBean
    private SubscriptionRepository subscriptionRepository;

    @Autowired
    private MockMvcTester mvc;

    @Test
    void get_whenExisting_thenReturnCorrect() {
        Subscription sub = createSub(ChannelType.WEBHOOK, "/test");
        when(subscriptionRepository.findById(sub.getId())).thenReturn(Optional.of(sub));

        assertThat(mvc.get().uri(SUBSCRIPTION_BY_ID, sub.getId()))
                .hasStatusOk()
                .bodyJson()
                .isLenientlyEqualTo("""
                        {"id":"%s","eventType":"%s","channel":"WEBHOOK","target":"/test"}
                        """.formatted(sub.getId(), EVENT_TYPE));
    }

    @Test
    void list_whenNoFilter_thenReturnsAll() {
        when(subscriptionRepository.findAll()).thenReturn(List.of(
                createSub("order.created", ChannelType.EMAIL, "a@b.com"),
                createSub("order.shipped", ChannelType.WEBHOOK, "/hook")));

        assertThat(mvc.get().uri(SUBSCRIPTION))
                .hasStatusOk()
                .bodyJson()
                .extractingPath("$").asArray().hasSize(2);

        verify(subscriptionRepository, never()).findByEventType(any());
    }

    @Test
    void list_whenFilteredByEventType_thenReturnsOnlyMatching() {
        when(subscriptionRepository.findByEventType("order.created")).thenReturn(List.of(
                createSub("order.created", ChannelType.EMAIL, "a@b.com")));

        assertThat(mvc.get().uri(SUBSCRIPTION + "?eventType={type}", "order.created"))
                .hasStatusOk()
                .bodyJson()
                .extractingPath("$[0].eventType").isEqualTo("order.created");

        verify(subscriptionRepository, never()).findAll();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"eventType\":\"\",\"channel\":\"EMAIL\",\"target\":\"a@b.com\"}",
            "{\"eventType\":\"   \",\"channel\":\"EMAIL\",\"target\":\"a@b.com\"}",
            "{\"eventType\":\"order.created\",\"channel\":\"EMAIL\",\"target\":\"\"}",
            "{\"eventType\":\"order.created\",\"channel\":\"EMAIL\"}",
            "{\"eventType\":\"order.created\",\"target\":\"a@b.com\"}",
            "{\"eventType\":\"order.created\",\"channel\":\"SMS\",\"target\":\"a@b.com\"}",
            "{}",
            "not json"
    })
    void post_whenInvalidBody_thenReturns400AndNothingSaved(String body) {
        assertThat(mvc.post().uri(SUBSCRIPTION)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
                .hasStatus(HttpStatus.BAD_REQUEST);

        verifyNoInteractions(subscriptionRepository);
    }

    @Test
    void get_whenMissing_thenReturns404() {
        UUID fake = UUID.randomUUID();
        when(subscriptionRepository.findById(fake)).thenReturn(Optional.empty());

        assertThat(mvc.get().uri(SUBSCRIPTION_BY_ID, fake))
                .hasStatus(HttpStatus.NOT_FOUND);
    }

    @Test
    void get_whenMalformedId_thenReturns400() {
        assertThat(mvc.get().uri(SUBSCRIPTION_BY_ID, "fake"))
                .hasStatus(HttpStatus.BAD_REQUEST);

        verifyNoInteractions(subscriptionRepository);
    }

    @Test
    void delete_whenExisting_thenReturns204() {
        UUID id = UUID.randomUUID();

        assertThat(mvc.delete().uri(SUBSCRIPTION_BY_ID, id))
                .hasStatus(HttpStatus.NO_CONTENT);

        verify(subscriptionRepository).deleteById(id);
    }

    @Test
    void post_whenCorrect_thenCreatedWithLocation() {
        Subscription sub = createSub("order.created", ChannelType.EMAIL, "a@b.com");
        when(subscriptionRepository.save(any())).thenReturn(sub);

        MvcTestResult result = mvc.post().uri(SUBSCRIPTION)
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"eventType":"order.created","channel":"EMAIL","target":"a@b.com"}
                        """)
                .exchange();

        assertThat(result)
                .hasStatus(HttpStatus.CREATED)
                .headers().hasValue("Location", "http://localhost/subscriptions/" + sub.getId());
        assertThat(result)
                .bodyJson()
                .extractingPath("$.eventType").isEqualTo("order.created");

        ArgumentCaptor<Subscription> captor = ArgumentCaptor.forClass(Subscription.class);
        verify(subscriptionRepository).save(captor.capture());
        Subscription passedToSave = captor.getValue();
        assertThat(passedToSave.getEventType()).isEqualTo("order.created");
        assertThat(passedToSave.getChannel()).isEqualTo(ChannelType.EMAIL);
        assertThat(passedToSave.getTarget()).isEqualTo("a@b.com");
    }

    private Subscription createSub(ChannelType channel, String target) {
        return createSub(EVENT_TYPE, channel, target);
    }

    private Subscription createSub(String eventType, ChannelType channel, String target) {
        Subscription sub = new Subscription(eventType, channel, target);
        ReflectionTestUtils.setField(sub, "id", UUID.randomUUID());
        return sub;
    }
}
