package com.ridehail.driver;

import static com.ridehail.common.Events.*;

import jakarta.persistence.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.security.Principal;

@Entity @Table(name = "drivers")
class Driver {
    enum Status { AVAILABLE, ON_TRIP }
    @Id public String id;
    public String name;
    @Enumerated(EnumType.STRING) public Status status = Status.AVAILABLE;
}

interface DriverRepo extends JpaRepository<Driver, String> {}

@RestController
@RequestMapping("/drivers")
class DriverController {
    record Register(@NotBlank String id, @NotBlank String name) {}
    private final DriverRepo repo;
    DriverController(DriverRepo repo) { this.repo = repo; }

    @PostMapping Driver register(@RequestBody @Valid Register r, Principal principal) {
        requireOwnId(r.id(), principal);
        Driver d = new Driver(); d.id = r.id(); d.name = r.name(); return repo.save(d);
    }
    @GetMapping("/{id}") Driver get(@PathVariable String id, Principal principal) {
        requireOwnId(id, principal);
        return repo.findById(id).orElseThrow();
    }

    static void requireOwnId(String id, Principal principal) {
        if (principal != null && !id.equals(principal.getName()))
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "driver identity does not match token subject");
    }
}

@Component
class DriverEventListeners {
    private final DriverRepo repo;
    private final StringRedisTemplate redis;
    DriverEventListeners(DriverRepo repo, StringRedisTemplate redis) { this.repo = repo; this.redis = redis; }

    /** Turn the short-lived reservation into a durable "busy" marker + persist status. Idempotent. */
    @KafkaListener(topics = DRIVER_ASSIGNED, groupId = "driver-service")
    @Transactional
    void onAssigned(String msg) {
        DriverAssigned e = fromJson(msg, DriverAssigned.class);
        redis.opsForValue().set("driver:{" + e.driverId() + "}:busy", e.tripId().toString());
        Driver d = repo.findById(e.driverId()).orElseGet(() -> { Driver n = new Driver(); n.id = e.driverId(); n.name = e.driverId(); return n; });
        d.status = Driver.Status.ON_TRIP;
        repo.save(d);
    }

    @KafkaListener(topics = TRIP_COMPLETED, groupId = "driver-service")
    @Transactional
    void onCompleted(String msg) {
        TripCompleted e = fromJson(msg, TripCompleted.class);
        redis.delete(java.util.List.of("driver:{" + e.driverId() + "}:busy", "driver:{" + e.driverId() + "}:reserved"));
        repo.findById(e.driverId()).ifPresent(d -> d.status = Driver.Status.AVAILABLE);
    }
}
