package com.ridehail.dispatch;

import static com.ridehail.common.Events.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

class NoDriverAvailableException extends RuntimeException {
    final UUID tripId;
    NoDriverAvailableException(UUID tripId) { super("no driver available for trip " + tripId); this.tripId = tripId; }
}

/** Finds + atomically reserves the best candidate driver. */
@Component
class Dispatcher {
    static final String GEO = "{drivers}:geo", SEEN = "{drivers}:seen";
    static final long FRESH_MS = 30_000;
    static final int RESERVATION_TTL_SEC = 120;

    /** Reserve if driver not busy and not reserved by another trip. Idempotent per tripId. */
    private static final RedisScript<Long> RESERVE = RedisScript.of("""
        local busy = redis.call('GET', KEYS[2])
        if busy and busy ~= ARGV[1] then return 0 end
        local v = redis.call('GET', KEYS[1])
        if not v then redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2]) return 1 end
        if v == ARGV[1] then return 1 end
        return 0""", Long.class);

    private final StringRedisTemplate redis;
    Dispatcher(StringRedisTemplate redis) { this.redis = redis; }

    Optional<String> assign(UUID tripId, double lat, double lng, Set<String> excluded) {
        GeoReference<String> ref = GeoReference.fromCoordinate(lng, lat);
        var nearby = redis.opsForGeo().search(GEO, ref, new Distance(5, Metrics.KILOMETERS),
                GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().sortAscending().limit(25));
        if (nearby == null) return Optional.empty();

        long freshAfter = System.currentTimeMillis() - FRESH_MS;
        for (var hit : nearby) {
            String id = hit.getContent().getName();
            if (excluded.contains(id)) continue;                 // already rejected/timed out this trip
            Double seen = redis.opsForZSet().score(SEEN, id);
            if (seen == null || seen < freshAfter) continue;     // stale location
            Long ok = redis.execute(RESERVE,
                    List.of("driver:{" + id + "}:reserved", "driver:{" + id + "}:busy"),
                    tripId.toString(), String.valueOf(RESERVATION_TTL_SEC));
            if (ok != null && ok == 1L) return Optional.of(id);
        }
        return Optional.empty();
    }
}

/**
 * Offer state machine per trip, stored in Redis:
 *   trip:{id}:current  = driverId (OFFERED) | ASSIGNED:driverId | FAILED
 *   trip:{id}:excluded = set of drivers who rejected / timed out
 *   {offers}:pending   = zset "tripId|driverId" -> deadline (drives the timeout sweeper)
 * Every transition is guarded by `current`, so duplicate or stale messages are no-ops.
 */
@Component
class OfferService {
    static final String PENDING = "{offers}:pending";
    static final int MAX_OFFERS = 5;

    private static final RedisScript<Long> RELEASE = RedisScript.of(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0", Long.class);

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafka;
    private final Dispatcher dispatcher;
    private final long offerTtlMs;

    OfferService(StringRedisTemplate redis, KafkaTemplate<String, String> kafka, Dispatcher dispatcher,
                 @Value("${dispatch.offer-ttl-ms:20000}") long offerTtlMs) {
        this.redis = redis; this.kafka = kafka; this.dispatcher = dispatcher; this.offerTtlMs = offerTtlMs;
    }

    static String current(UUID t) { return "trip:{" + t + "}:current"; }
    static String excluded(UUID t) { return "trip:{" + t + "}:excluded"; }
    static String pickup(UUID t) { return "trip:{" + t + "}:pickup"; }

    void start(TripCreated t) throws Exception {
        if (Boolean.TRUE.equals(redis.hasKey(current(t.tripId())))) return;          // duplicate trip.created
        redis.opsForValue().set(pickup(t.tripId()), t.pickupLat() + "," + t.pickupLng(), Duration.ofHours(24));
        offer(t.tripId());
    }

    void offer(UUID tripId) throws Exception {
        String[] p = redis.opsForValue().get(pickup(tripId)).split(",");
        double lat = Double.parseDouble(p[0]), lng = Double.parseDouble(p[1]);
        Set<String> excl = redis.opsForSet().members(excluded(tripId));
        String driverId = dispatcher.assign(tripId, lat, lng, excl == null ? Set.of() : excl)
                .orElseThrow(() -> new NoDriverAvailableException(tripId));      // backoff retry, then dispatch.failed

        long now = System.currentTimeMillis(), expires = now + offerTtlMs;
        redis.opsForValue().set(current(tripId), driverId, Duration.ofHours(24));
        redis.opsForZSet().add(PENDING, tripId + "|" + driverId, expires);         // crash after this => sweeper re-drives
        kafka.send(DRIVER_OFFERED, tripId.toString(),
                toJson(new DriverOffered(tripId, driverId, lat, lng, expires, now))).get();
    }

    void onResponse(OfferResponded r) throws Exception {
        UUID tripId = r.tripId();
        redis.opsForZSet().remove(PENDING, tripId + "|" + r.driverId());             // stops the sweeper; state check below is the truth
        if (!r.driverId().equals(redis.opsForValue().get(current(tripId)))) return;  // stale or duplicate

        if (r.accepted()) {
            kafka.send(DRIVER_ASSIGNED, tripId.toString(),
                    toJson(new DriverAssigned(tripId, r.driverId(), System.currentTimeMillis()))).get();
            redis.opsForValue().set(current(tripId), "ASSIGNED:" + r.driverId(), Duration.ofHours(24));
            return;
        }
        redis.opsForSet().add(excluded(tripId), r.driverId());
        redis.expire(excluded(tripId), Duration.ofHours(24));
        redis.execute(RELEASE, List.of("driver:{" + r.driverId() + "}:reserved"), tripId.toString());

        Long rejected = redis.opsForSet().size(excluded(tripId));
        if (rejected != null && rejected >= MAX_OFFERS) {
            kafka.send(DISPATCH_FAILED, tripId.toString(),
                    toJson(new DispatchFailed(tripId, "MAX_OFFERS_REACHED", System.currentTimeMillis()))).get();
            redis.opsForValue().set(current(tripId), "FAILED", Duration.ofHours(24));
            return;
        }
        offer(tripId);                                                               // next best driver
    }

    /** Unanswered offers become TIMEOUT rejections. Safe with many pods: handling is idempotent. */
    @Scheduled(fixedDelay = 2_000)
    void sweepExpired() {
        Set<String> expired = redis.opsForZSet().rangeByScore(PENDING, 0, System.currentTimeMillis());
        if (expired == null) return;
        for (String m : expired) {
            String[] parts = m.split("\\|");
            UUID tripId = UUID.fromString(parts[0]);
            kafka.send(OFFER_RESPONDED, tripId.toString(),
                    toJson(new OfferResponded(tripId, parts[1], false, "TIMEOUT", System.currentTimeMillis())));
        }
    }
}

@Component
class DispatchListeners {
    private final OfferService offers;
    DispatchListeners(OfferService offers) { this.offers = offers; }

    @KafkaListener(topics = TRIP_CREATED, groupId = "dispatch")
    void onTripCreated(String msg) throws Exception { offers.start(fromJson(msg, TripCreated.class)); }

    @KafkaListener(topics = OFFER_RESPONDED, groupId = "dispatch")
    void onOfferResponded(String msg) throws Exception { offers.onResponse(fromJson(msg, OfferResponded.class)); }
}
