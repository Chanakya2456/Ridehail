package com.ridehail.dispatch;

import static com.ridehail.common.Events.*;

import java.util.stream.Stream;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Creates all topics (6 partitions => up to 6 dispatch pods consume in parallel). */
@Configuration
class TopicsConfig {
    @Bean KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
            Stream.of(DRIVER_LOCATION_UPDATED, TRIP_CREATED, DRIVER_ASSIGNED, TRIP_COMPLETED,
                      DRIVER_OFFERED, OFFER_RESPONDED, DISPATCH_FAILED,
                      TRIP_CREATED + ".DLT", DRIVER_ASSIGNED + ".DLT", TRIP_COMPLETED + ".DLT",
                      DRIVER_OFFERED + ".DLT", OFFER_RESPONDED + ".DLT", DISPATCH_FAILED + ".DLT")
                .map(n -> TopicBuilder.name(n).partitions(6).replicas(1).build())
                .toArray(NewTopic[]::new));
    }
}
