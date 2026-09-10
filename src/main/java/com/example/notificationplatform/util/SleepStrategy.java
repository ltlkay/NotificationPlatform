package com.example.notificationplatform.util;

import org.springframework.stereotype.Component;

@Component
public class SleepStrategy implements WaitStrategy{

    @Override
    public void stay(long delay) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
