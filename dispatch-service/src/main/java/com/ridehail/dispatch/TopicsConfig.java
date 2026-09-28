package com.ridehail.dispatch;

import static com.ridehail.common.Events.*;

import java.util.stream.Stream;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Creates the source and DLT topics; six partitions allow six concurrent consumers. */
@Configuration
class TopicsConfig {
    @Bean KafkaAdmin.NewTopics topics(@Value("${KAFKA_TOPIC_REPLICAS:1}") int replicas) {
        return new KafkaAdmin.NewTopics(
            Stream.of(DRIVER_LOCATION_UPDATED, TRIP_CREATED, DRIVER_ASSIGNED, TRIP_COMPLETED,
                      DRIVER_OFFERED, OFFER_RESPONDED, DISPATCH_FAILED,
                      TRIP_CREATED + ".DLT", DRIVER_ASSIGNED + ".DLT", TRIP_COMPLETED + ".DLT",
                      DRIVER_OFFERED + ".DLT", OFFER_RESPONDED + ".DLT", DISPATCH_FAILED + ".DLT")
                .map(n -> {
                    var topic = TopicBuilder.name(n).partitions(6).replicas(replicas);
                    if (n.endsWith(".DLT")) topic.config(TopicConfig.RETENTION_MS_CONFIG, "1209600000"); // 14 days
                    return topic.build();
                })
                .toArray(NewTopic[]::new));
    }
}
