package com.example.notificationplatform.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class TestLogAppender extends AppenderBase<ILoggingEvent> implements AutoCloseable{
    private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();
    private Logger logger;

    @Override
    protected void append(ILoggingEvent eventObject){
        events.add(eventObject);
    }

    public List<ILoggingEvent> getEvents() {
        return List.copyOf(events);
    }

    public static TestLogAppender attachTo(Class<?> loggedClass){
        TestLogAppender appender = new TestLogAppender();
        appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        appender.start();

        appender.logger = (Logger) LoggerFactory.getLogger(loggedClass);
        appender.logger.addAppender(appender);
        return appender;
    }

    private void detach() {
        logger.detachAppender(this);
        this.stop();
    }

    public void assertNoErrorLogged() {
        assertThat(getEvents()).extracting(ILoggingEvent::getLevel).doesNotContain(Level.ERROR);
    }

    public void assertNotLogged(Level level, String message) {
        assertThat(getEvents()).extracting(ILoggingEvent::getLevel, ILoggingEvent::getFormattedMessage)
                .doesNotContain(tuple(level, message));
    }

    public void assertLogged(Level level, String message) {
        assertThat(getEvents()).extracting(ILoggingEvent::getLevel, ILoggingEvent::getFormattedMessage)
                .contains(tuple(level, message));
    }

    public List<ILoggingEvent> eventsAt(Level level) {
        return getEvents().stream().filter(event -> event.getLevel().equals(level)).toList();
    }

    public List<String> messages() {
        return getEvents().stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    public void clear() {
        events.clear();
    }

    @Override
    public void close(){
        detach();
    }
}
