package com.ridehail.dispatch;

import static com.ridehail.common.Events.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
class KafkaErrorConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);
    /** Retry 10x every 2s. If still no driver: tell trip-service (dispatch.failed), then dead-letter. */
    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
            @Value("${dispatch.retry-interval-ms:2000}") long intervalMs,
            @Value("${dispatch.retry-attempts:10}") long attempts, MeterRegistry metrics) {
        var dlt = new DeadLetterPublishingRecoverer(template);
        Counter dltCounter = Counter.builder("ridehail.kafka.dlt.records").tag("service", "dispatch").register(metrics);
        ConsumerRecordRecoverer recoverer = (record, ex) -> {
            log.error("Kafka record sent to DLT topic={} partition={} key={}", record.topic(), record.partition(), record.key(), ex);
            dltCounter.increment();
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof NoDriverAvailableException n) {
                template.send(DISPATCH_FAILED, n.tripId.toString(),
                        toJson(new DispatchFailed(n.tripId, "NO_DRIVER_AVAILABLE", System.currentTimeMillis())));
            }
            dlt.accept(record, ex);
        };
        var h = new DefaultErrorHandler(recoverer, new FixedBackOff(intervalMs, attempts));
        h.addNotRetryableExceptions(IllegalArgumentException.class);
        return h;
    }
}
