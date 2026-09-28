package com.ridehail.dispatch;

import static com.ridehail.common.Events.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Boots dispatch-service against real Kafka + Redis. Needs Docker. Each test uses its own latitude band (>5km apart). */
@SpringBootTest(properties = {"dispatch.offer-ttl-ms=3000", "dispatch.retry-interval-ms=200", "dispatch.retry-attempts=5"})
@Testcontainers(disabledWithoutDocker = true)
@Import(DispatchFlowTest.CollectorConfig.class)
class DispatchFlowTest {

    @Container static KafkaContainer kafkaC = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    @Container static GenericContainer<?> redisC = new GenericContainer<>("redis:7").withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", kafkaC::getBootstrapServers);
        r.add("spring.data.redis.host", redisC::getHost);
        r.add("spring.data.redis.port", () -> redisC.getMappedPort(6379));
    }

    public static class Collector {
        final Queue<ConsumerRecord<String, String>> records = new ConcurrentLinkedQueue<>();
        @KafkaListener(topics = {DRIVER_OFFERED, DRIVER_ASSIGNED, DISPATCH_FAILED}, groupId = "it-collector",
                       properties = "auto.offset.reset=earliest")
        public void on(ConsumerRecord<String, String> r) { records.add(r); }
    }
    @TestConfiguration static class CollectorConfig { @Bean Collector collector() { return new Collector(); } }

    @Autowired StringRedisTemplate redis;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired Collector collector;

    // ---- scenarios -------------------------------------------------------------------------------------------

    @Test void rejectThenAccept() {
        UUID trip = UUID.randomUUID(); String near = id("near"), far = id("far");
        seed(near, 10.000, 72.0); seed(far, 10.005, 72.0);                    // ~0m and ~550m from pickup
        requestTrip(trip, 10.0, 72.0);

        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(offeredTo(trip)).containsExactly(near));
        respond(trip, near, false);
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(offeredTo(trip)).containsExactly(near, far));
        assertThat(redis.hasKey("driver:{" + near + "}:reserved")).isFalse();  // rejected driver is released immediately

        respond(trip, far, true);
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(assignedTo(trip)).containsExactly(far));
    }

    @Test void unansweredOfferTimesOutToNextDriver() {
        UUID trip = UUID.randomUUID(); String near = id("near"), far = id("far");
        seed(near, 20.000, 72.0); seed(far, 20.005, 72.0);
        requestTrip(trip, 20.0, 72.0);
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(offeredTo(trip)).containsExactly(near));
        await().atMost(25, SECONDS).untilAsserted(() -> assertThat(offeredTo(trip)).containsExactly(near, far));
    }

    @Test void noDriversPublishesDispatchFailed() {
        UUID trip = UUID.randomUUID();
        requestTrip(trip, 30.0, 72.0);
        await().atMost(20, SECONDS).untilAsserted(() -> assertThat(failed(trip)).isTrue());
    }

    @Test void duplicateTripCreatedOffersOnce() {
        UUID trip = UUID.randomUUID(); String d = id("d");
        seed(d, 40.0, 72.0);
        requestTrip(trip, 40.0, 72.0);
        requestTrip(trip, 40.0, 72.0);
        await().pollDelay(2, SECONDS).atMost(15, SECONDS).untilAsserted(() -> assertThat(offeredTo(trip)).containsExactly(d));
    }

    @Test void twoTripsCannotShareOneDriver() {
        UUID t1 = UUID.randomUUID(), t2 = UUID.randomUUID(); String d = id("solo");
        seed(d, 50.0, 72.0);
        requestTrip(t1, 50.0, 72.0);
        requestTrip(t2, 50.0, 72.0);
        // exactly one trip gets the driver while the other fails (before the winner's own offer times out)
        await().atMost(20, SECONDS).untilAsserted(() -> {
            assertThat(offeredTo(t1).size() + offeredTo(t2).size()).isEqualTo(1);
            assertThat(failed(t1) ^ failed(t2)).isTrue();
        });
    }

    // ---- helpers ---------------------------------------------------------------------------------------------

    private static String id(String p) { return p + "-" + UUID.randomUUID().toString().substring(0, 8); }

    private void seed(String driver, double lat, double lng) {
        redis.opsForGeo().add("{drivers}:geo", new Point(lng, lat), driver);
        redis.opsForZSet().add("{drivers}:seen", driver, System.currentTimeMillis());
    }
    private void requestTrip(UUID t, double lat, double lng) {
        kafka.send(TRIP_CREATED, t.toString(), toJson(new TripCreated(t, lat, lng, System.currentTimeMillis())));
    }
    private void respond(UUID t, String driver, boolean accepted) {
        kafka.send(OFFER_RESPONDED, t.toString(), toJson(new OfferResponded(t, driver, accepted,
                accepted ? "ACCEPTED" : "REJECTED", System.currentTimeMillis())));
    }
    private List<String> offeredTo(UUID t) {
        return forTrip(DRIVER_OFFERED, t).map(v -> fromJson(v, DriverOffered.class).driverId()).toList();
    }
    private List<String> assignedTo(UUID t) {
        return forTrip(DRIVER_ASSIGNED, t).map(v -> fromJson(v, DriverAssigned.class).driverId()).toList();
    }
    private boolean failed(UUID t) { return forTrip(DISPATCH_FAILED, t).findAny().isPresent(); }
    private java.util.stream.Stream<String> forTrip(String topic, UUID t) {
        return collector.records.stream().filter(r -> r.topic().equals(topic) && t.toString().equals(r.key())).map(ConsumerRecord::value);
    }
}
