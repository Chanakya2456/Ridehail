package com.ridehail.driver;

import static com.ridehail.common.Events.*;

import java.time.Duration;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import java.security.Principal;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver-app facing API. In production push the offer (FCM/APNs/WebSocket); polling keeps this demo simple. */
@RestController
@RequestMapping("/drivers")
class OfferController {
    private static final Logger log = LoggerFactory.getLogger(OfferController.class);
    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafka;
    OfferController(StringRedisTemplate redis, KafkaTemplate<String, String> kafka) { this.redis = redis; this.kafka = kafka; }

    @GetMapping("/{id}/offer")
    ResponseEntity<String> pending(@PathVariable String id, Principal principal) {
        DriverController.requireOwnId(id, principal);
        String offer = redis.opsForValue().get("driver:{" + id + "}:offer");
        return offer == null ? ResponseEntity.notFound().build()
                : ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(offer);
    }

    @PostMapping("/{id}/offers/{tripId}/accept")
    ResponseEntity<Void> accept(@PathVariable String id, @PathVariable UUID tripId, Principal principal) {
        DriverController.requireOwnId(id, principal); return respond(id, tripId, true);
    }

    @PostMapping("/{id}/offers/{tripId}/reject")
    ResponseEntity<Void> reject(@PathVariable String id, @PathVariable UUID tripId, Principal principal) {
        DriverController.requireOwnId(id, principal); return respond(id, tripId, false);
    }

    private ResponseEntity<Void> respond(String id, UUID tripId, boolean accepted) {
        // offer still valid only while this driver holds the reservation for this trip
        if (!tripId.toString().equals(redis.opsForValue().get("driver:{" + id + "}:reserved")))
            return ResponseEntity.status(409).build();
        String payload = redis.opsForValue().get("driver:{" + id + "}:offer");
        if (payload == null) return ResponseEntity.status(409).build();
        DriverOffered offer = fromJson(payload, DriverOffered.class);
        if (!tripId.equals(offer.tripId()) || offer.expiresAt() <= System.currentTimeMillis())
            return ResponseEntity.status(409).build();
        try {
            kafka.send(OFFER_RESPONDED, tripId.toString(), toJson(
                    new OfferResponded(tripId, id, accepted, accepted ? "ACCEPTED" : "REJECTED", System.currentTimeMillis())))
                    .get(30, TimeUnit.SECONDS);
            redis.delete("driver:{" + id + "}:offer");
            return ResponseEntity.accepted().build();   // final outcome: GET /trips/{id}
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing offer response tripId={} driverId={}", tripId, id, e);
            return ResponseEntity.status(503).build();
        } catch (Exception e) {
            log.warn("Could not publish offer response tripId={} driverId={}", tripId, id, e);
            return ResponseEntity.status(503).build();
        }
    }
}

@Component
class OfferedListener {
    private final StringRedisTemplate redis;
    OfferedListener(StringRedisTemplate redis) { this.redis = redis; }

    @KafkaListener(topics = DRIVER_OFFERED, groupId = "driver-service")
    void onOffered(String msg) {
        DriverOffered o = fromJson(msg, DriverOffered.class);
        redis.opsForValue().set("driver:{" + o.driverId() + "}:offer", msg, Duration.ofSeconds(30));
    }
}
