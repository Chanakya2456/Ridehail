package com.ridehail.trip;

import static com.ridehail.common.Events.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import java.security.Principal;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.dao.DataIntegrityViolationException;
import io.micrometer.core.instrument.MeterRegistry;

@Service
class TripService {
    private final TripRepo trips;
    private final OutboxRepo outbox;
    TripService(TripRepo trips, OutboxRepo outbox) { this.trips = trips; this.outbox = outbox; }

    @Transactional
    Trip create(TripController.CreateTrip r, String passengerId, String idempotencyKey) {
        if (idempotencyKey != null) {
            Trip previous = trips.findByPassengerIdAndIdempotencyKey(passengerId, idempotencyKey).orElse(null);
            if (previous != null) return previous;
        }
        Trip t = new Trip();
        t.id = UUID.randomUUID(); t.passengerId = passengerId;
        t.idempotencyKey = idempotencyKey;
        t.pickupLat = r.pickupLat(); t.pickupLng = r.pickupLng(); t.dropLat = r.dropLat(); t.dropLng = r.dropLng();
        t.status = Trip.Status.REQUESTED; t.createdAt = Instant.now();
        trips.save(t);
        enqueue(TRIP_CREATED, t.id, toJson(new TripCreated(t.id, t.pickupLat, t.pickupLng, System.currentTimeMillis())));
        return t;
    }

    /** Idempotent: only the first driver.assigned for a REQUESTED trip has effect. */
    @Transactional
    void onDriverAssigned(DriverAssigned e) {
        trips.findById(e.tripId()).ifPresent(t -> {
            if (t.status == Trip.Status.REQUESTED) { t.status = Trip.Status.ASSIGNED; t.driverId = e.driverId(); }
        });
    }

    @Transactional
    void onDispatchFailed(DispatchFailed e) {
        trips.findById(e.tripId()).ifPresent(t -> { if (t.status == Trip.Status.REQUESTED) t.status = Trip.Status.NO_DRIVER; });
    }

    @Transactional
    Trip complete(UUID id) {
        Trip t = trips.findById(id).orElseThrow();
        if (t.status == Trip.Status.ASSIGNED) {
            t.status = Trip.Status.COMPLETED;
            enqueue(TRIP_COMPLETED, t.id, toJson(new TripCompleted(t.id, t.driverId, System.currentTimeMillis())));
        }
        return t;
    }

    private void enqueue(String topic, UUID key, String payload) {
        OutboxEvent o = new OutboxEvent();
        o.id = UUID.randomUUID(); o.topic = topic; o.msgKey = key.toString();
        o.payload = payload; o.createdAt = Instant.now(); o.sent = false;
        outbox.save(o);
    }

    Trip get(UUID id) { return trips.findById(id).orElseThrow(); }
    java.util.Optional<Trip> findByPassengerAndKey(String passengerId, String key) {
        return trips.findByPassengerIdAndIdempotencyKey(passengerId, key);
    }
}

@RestController
@RequestMapping("/trips")
class TripController {
    record CreateTrip(@DecimalMin("-90.0") @DecimalMax("90.0") double pickupLat,
                      @DecimalMin("-180.0") @DecimalMax("180.0") double pickupLng,
                      @DecimalMin("-90.0") @DecimalMax("90.0") double dropLat,
                      @DecimalMin("-180.0") @DecimalMax("180.0") double dropLng) {}
    private final TripService svc;
    TripController(TripService svc) { this.svc = svc; }

    @PostMapping ResponseEntity<Trip> create(@RequestBody @Valid CreateTrip r, Principal principal,
                                              @RequestHeader(value = "X-Passenger-Id", required = false) String localPassenger,
                                              @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        String passengerId = principal != null ? principal.getName() : localPassenger;
        if (passengerId == null || passengerId.isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "authenticated passenger required");
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > 200))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be 1-200 characters");
        try {
            return ResponseEntity.accepted().body(svc.create(r, passengerId, idempotencyKey));
        } catch (DataIntegrityViolationException concurrentRetry) {
            if (idempotencyKey == null) throw concurrentRetry;
            Trip previous = svc.findByPassengerAndKey(passengerId, idempotencyKey).orElseThrow(() -> concurrentRetry);
            return ResponseEntity.accepted().body(previous);
        }
    }
    @GetMapping("/{id}") Trip get(@PathVariable UUID id, Principal principal) {
        Trip t = svc.get(id);
        if (principal != null && !principal.getName().equals(t.passengerId) && !principal.getName().equals(t.driverId))
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "trip is not owned by this identity");
        return t;
    }
    @PatchMapping("/{id}/complete") Trip complete(@PathVariable UUID id, Principal principal) {
        Trip t = svc.get(id);
        if (principal != null && !principal.getName().equals(t.driverId))
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "only the assigned driver can complete this trip");
        return svc.complete(id);
    }
}

@Component
class DriverAssignedListener {
    private final TripService svc;
    DriverAssignedListener(TripService svc) { this.svc = svc; }

    @KafkaListener(topics = DRIVER_ASSIGNED, groupId = "trip-service")
    void on(String msg) { svc.onDriverAssigned(fromJson(msg, DriverAssigned.class)); }

    @KafkaListener(topics = DISPATCH_FAILED, groupId = "trip-service")
    void onFailed(String msg) { svc.onDispatchFailed(fromJson(msg, DispatchFailed.class)); }
}

@Component
class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxRepo repo;
    private final KafkaTemplate<String, String> kafka;
    OutboxPublisher(OutboxRepo repo, KafkaTemplate<String, String> kafka, MeterRegistry metrics) {
        this.repo = repo; this.kafka = kafka;
        metrics.gauge("ridehail.outbox.pending", repo, OutboxRepo::countBySentFalse);
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publish() {
        for (OutboxEvent e : repo.lockBatch()) {
            try {
                kafka.send(e.topic, e.msgKey, e.payload).get(30, TimeUnit.SECONDS);
                e.sent = true;                       // at-least-once; consumers are idempotent
            } catch (Exception ex) {
                log.warn("outbox publish failed, will retry: {}", ex.toString());
                break;                               // preserve ordering
            }
        }
    }
}
