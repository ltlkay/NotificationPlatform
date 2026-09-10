package com.example.notificationplatform.util;

public class SkipStrategy implements WaitStrategy{
    @Override
    public void stay(long delay) {
        System.out.println("Mocking delay");
    }
}
