package com.ridehail.trip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOffWithMaxRetries;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Configuration
class KafkaErrorConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);

    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, MeterRegistry metrics) {
        Counter dltCounter = Counter.builder("ridehail.kafka.dlt.records").tag("service", "trip").register(metrics);
        var recoverer = new DeadLetterPublishingRecoverer(template, (record, ex) ->
                new org.apache.kafka.common.TopicPartition(record.topic() + ".DLT", record.partition()));
        var backoff = new ExponentialBackOffWithMaxRetries(8);
        backoff.setInitialInterval(500L);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(30_000L);
        var handler = new DefaultErrorHandler((record, ex) -> {
            log.error("Kafka record sent to DLT topic={} partition={} key={}", record.topic(), record.partition(), record.key(), ex);
            dltCounter.increment();
            recoverer.accept(record, ex);
        }, backoff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
}
