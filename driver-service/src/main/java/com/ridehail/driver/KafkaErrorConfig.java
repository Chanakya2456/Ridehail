package com.ridehail.driver;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

@Configuration
class KafkaErrorConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);
    /** Retry 10x every 2s, then publish to <topic>.DLT. Bad payloads skip retries. */
    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, MeterRegistry metrics) {
        var dlt = new DeadLetterPublishingRecoverer(template);
        Counter dltCounter = Counter.builder("ridehail.kafka.dlt.records").tag("service", "driver").register(metrics);
        ConsumerRecordRecoverer recoverer = (record, ex) -> {
            log.error("Kafka record sent to DLT topic={} partition={} key={}", record.topic(), record.partition(), record.key(), ex);
            dltCounter.increment();
            dlt.accept(record, ex);
        };
        var h = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 10));
        h.addNotRetryableExceptions(IllegalArgumentException.class);
        return h;
    }
}
