package com.ridehail.trip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;

@Configuration
class KafkaErrorConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);

    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, MeterRegistry metrics,
            @Value("${kafka.retry.max-retries:5}") int maxRetries,
            @Value("${kafka.retry.initial-interval-ms:500}") long initialIntervalMs,
            @Value("${kafka.retry.max-interval-ms:4000}") long maxIntervalMs) {
        Counter dltCounter = Counter.builder("ridehail.kafka.dlt.records").tag("service", "trip").register(metrics);
        var recoverer = new DeadLetterPublishingRecoverer(template, (record, ex) ->
                new org.apache.kafka.common.TopicPartition(record.topic() + ".DLT", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(30));
        var backoff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backoff.setInitialInterval(initialIntervalMs);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(maxIntervalMs);
        var handler = new DefaultErrorHandler((record, ex) -> {
            recoverer.accept(record, ex);
            dltCounter.increment();
            log.error("Kafka record sent to DLT topic={} partition={} key={}", record.topic(), record.partition(), record.key(), ex);
        }, backoff);
        Counter retryCounter = Counter.builder("ridehail.kafka.retries").tag("service", "trip").register(metrics);
        handler.setRetryListeners((record, failure, attempt) -> {
            if (attempt > 1) retryCounter.increment();
        });
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
}
