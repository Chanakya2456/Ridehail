package com.ridehail.driver;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import java.time.Duration;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
class KafkaErrorConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);
    /** Retry transient failures with bounded exponential backoff, then publish to <topic>.DLT. */
    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, MeterRegistry metrics) {
        var dlt = new DeadLetterPublishingRecoverer(template);
        dlt.setFailIfSendResultIsError(true);
        dlt.setWaitForSendResultTimeout(Duration.ofSeconds(30));
        Counter dltCounter = Counter.builder("ridehail.kafka.dlt.records").tag("service", "driver").register(metrics);
        ConsumerRecordRecoverer recoverer = (record, ex) -> {
            dlt.accept(record, ex);
            dltCounter.increment();
            log.error("Kafka record sent to DLT topic={} partition={} key={}", record.topic(), record.partition(), record.key(), ex);
        };
        var backoff = new ExponentialBackOffWithMaxRetries(5);
        backoff.setInitialInterval(500L);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(4_000L);
        var h = new DefaultErrorHandler(recoverer, backoff);
        Counter retryCounter = Counter.builder("ridehail.kafka.retries").tag("service", "driver").register(metrics);
        h.setRetryListeners((record, failure, attempt) -> {
            if (attempt > 1) retryCounter.increment();
        });
        h.addNotRetryableExceptions(IllegalArgumentException.class);
        return h;
    }
}
