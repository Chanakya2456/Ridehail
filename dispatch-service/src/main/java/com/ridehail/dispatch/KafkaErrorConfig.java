package com.ridehail.dispatch;

import static com.ridehail.common.Events.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
class KafkaErrorConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);
    /** Retry transient failures with bounded exponential backoff; no-driver exhaustion publishes a business failure. */
    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
            @Value("${dispatch.retry-max-retries:5}") int maxRetries,
            @Value("${dispatch.retry-initial-interval-ms:500}") long initialIntervalMs,
            @Value("${dispatch.retry-max-interval-ms:4000}") long maxIntervalMs,
            MeterRegistry metrics, StringRedisTemplate redis) {
        var dlt = new DeadLetterPublishingRecoverer(template);
        dlt.setFailIfSendResultIsError(true);
        dlt.setWaitForSendResultTimeout(Duration.ofSeconds(30));
        Counter dltCounter = Counter.builder("ridehail.kafka.dlt.records").tag("service", "dispatch").register(metrics);
        ConsumerRecordRecoverer recoverer = (record, ex) -> {
            Throwable cause = ex;
            while (cause != null && !(cause instanceof NoDriverAvailableException)) cause = cause.getCause();
            if (cause instanceof NoDriverAvailableException n) {
                try {
                    template.send(DISPATCH_FAILED, n.tripId.toString(),
                            toJson(new DispatchFailed(n.tripId, "NO_DRIVER_AVAILABLE", System.currentTimeMillis())))
                            .get(30, TimeUnit.SECONDS);
                    redis.opsForValue().set(OfferService.current(n.tripId), "FAILED", Duration.ofHours(24));
                    log.warn("Dispatch exhausted driver search tripId={}", n.tripId);
                    return; // no driver is a completed business outcome, not a poison message
                } catch (Exception publishFailure) {
                    throw new IllegalStateException("could not publish dispatch.failed", publishFailure);
                }
            }
            dlt.accept(record, ex);
            dltCounter.increment();
            log.error("Kafka record sent to DLT topic={} partition={} key={}", record.topic(), record.partition(), record.key(), ex);
        };
        var backoff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backoff.setInitialInterval(initialIntervalMs);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(maxIntervalMs);
        var h = new DefaultErrorHandler(recoverer, backoff);
        Counter retryCounter = Counter.builder("ridehail.kafka.retries").tag("service", "dispatch").register(metrics);
        h.setRetryListeners((record, failure, attempt) -> {
            if (attempt > 1) retryCounter.increment();
        });
        h.addNotRetryableExceptions(IllegalArgumentException.class);
        return h;
    }
}
