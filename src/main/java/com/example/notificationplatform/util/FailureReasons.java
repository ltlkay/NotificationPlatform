package com.example.notificationplatform.util;

public final class FailureReasons {

    private FailureReasons() {}

    /** The throwable's message, or its class simple name when the message is null or blank. */
    public static String reasonOf(Throwable e) {
        return (e.getMessage() != null && !e.getMessage().isBlank())
                ? e.getMessage()
                : e.getClass().getSimpleName();
    }
}
