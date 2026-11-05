package com.example.notificationplatform.delivery;

import com.example.notificationplatform.util.DeliveryStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryResultTest {
    private static final String REASON = "Connection refused";

    @Test
    void success_hasSuccessStatusNoReasonAndAttemptCount() {
        DeliveryResult result = DeliveryResult.success(3);

        assertThat(result.status()).isEqualTo(DeliveryStatus.SUCCESS);
        assertThat(result.failureReason()).isNull();
        assertThat(result.attemptCount()).isEqualTo(3);
    }

    @Test
    void success_acceptsMinimumAttemptCountOfOne() {
        assertThat(DeliveryResult.success(1).attemptCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void success_whenAttemptCountBelowOne_throws(int attemptCount) {
        assertThatThrownBy(() -> DeliveryResult.success(attemptCount))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attemptCount");
    }

    @Test
    void failure_keepsReasonAndAttemptCountUnchanged() {
        DeliveryResult result = DeliveryResult.failure(REASON, 5);

        assertThat(result.status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(result.failureReason()).isEqualTo(REASON);
        assertThat(result.attemptCount()).isEqualTo(5);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t", "\n"})
    void failure_whenReasonNullOrBlank_usesUnknownReason(String reason) {
        DeliveryResult result = DeliveryResult.failure(reason, 2);

        assertThat(result.status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(result.failureReason()).isEqualTo(DeliveryResult.UNKNOWN_REASON);
        assertThat(result.attemptCount()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void failure_whenAttemptCountBelowOne_throws(int attemptCount) {
        assertThatThrownBy(() -> DeliveryResult.failure(REASON, attemptCount))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attemptCount");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void constructor_whenFailedWithNullOrBlankReason_throws(String reason) {
        assertThatThrownBy(() -> new DeliveryResult(DeliveryStatus.FAILED, reason, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureReason");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", REASON})
    void constructor_whenSuccessWithReason_throws(String reason) {
        assertThatThrownBy(() -> new DeliveryResult(DeliveryStatus.SUCCESS, reason, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureReason");
    }

    @Test
    void constructor_whenStatusNull_throwsNullPointer() {
        assertThatThrownBy(() -> new DeliveryResult(null, null, 1))
                .isInstanceOf(NullPointerException.class);
    }
}
