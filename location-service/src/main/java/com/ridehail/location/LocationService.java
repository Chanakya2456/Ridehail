package com.ridehail.location;

import static com.ridehail.common.Events.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import java.security.Principal;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/drivers")
class LocationController {
    record LocationRequest(@DecimalMin("-90") @DecimalMax("90") double lat,
                           @DecimalMin("-180") @DecimalMax("180") double lng,
                           Long ts) {}

    private final LocationService service;
    LocationController(LocationService service) { this.service = service; }

    @PostMapping("/{driverId}/location")
    ResponseEntity<Void> update(@PathVariable String driverId, @RequestBody @Valid LocationRequest r, Principal principal) {
        if (principal != null && !driverId.equals(principal.getName()))
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "driver identity does not match token subject");
        long now = System.currentTimeMillis();
        if (r.ts() != null && (r.ts() < now - 60_000 || r.ts() > now + 60_000))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "location timestamp must be within 60 seconds of server time");
        service.update(driverId, r.lat(), r.lng(), r.ts() == null ? now : r.ts());
        return ResponseEntity.accepted().build();
    }
}

@Service
class LocationService {
    // Hash tag {drivers} keeps both keys in one slot so the Lua scripts work on Redis Cluster.
    static final String GEO = "{drivers}:geo", SEEN = "{drivers}:seen";

    /** Ignore out-of-order / stale updates: only apply if ts is newer than what we have. */
    private static final RedisScript<Long> UPSERT = RedisScript.of("""
        local cur = redis.call('ZSCORE', KEYS[2], ARGV[1])
        if cur and tonumber(cur) >= tonumber(ARGV[4]) then return 0 end
        redis.call('GEOADD', KEYS[1], ARGV[2], ARGV[3], ARGV[1])
        redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
        return 1""", Long.class);

    /** Atomically evict drivers not heard from since cutoff. */
    private static final RedisScript<Long> EVICT = RedisScript.of("""
        local s = redis.call('ZRANGEBYSCORE', KEYS[2], 0, ARGV[1])
        for _, m in ipairs(s) do redis.call('ZREM', KEYS[1], m); redis.call('ZREM', KEYS[2], m) end
        return #s""", Long.class);

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafka;

    LocationService(StringRedisTemplate redis, KafkaTemplate<String, String> kafka) {
        this.redis = redis; this.kafka = kafka;
    }

    void update(String driverId, double lat, double lng, long ts) {
        Long applied = redis.execute(UPSERT, List.of(GEO, SEEN),
                driverId, String.valueOf(lng), String.valueOf(lat), String.valueOf(ts));
        if (applied != null && applied == 1L) {
            // keyed by driverId => per-driver ordering within a partition
            kafka.send(DRIVER_LOCATION_UPDATED, driverId, toJson(new DriverLocationUpdated(driverId, lat, lng, ts)));
        }
    }

    @Scheduled(fixedDelay = 15_000)
    void evictStale() {
        redis.execute(EVICT, List.of(GEO, SEEN), String.valueOf(System.currentTimeMillis() - 60_000));
    }
}
