package com.ridehail.driver;

import static com.ridehail.common.Events.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.config.annotation.*;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/** ws://gateway/ws/drivers/{driverId}  — server pushes DriverOffered JSON; driver answers via the REST accept/reject. */
@Configuration
@EnableWebSocket
@EnableScheduling
class WebSocketConfig implements WebSocketConfigurer {
    private final DriverSocketHandler handler;
    WebSocketConfig(DriverSocketHandler handler) { this.handler = handler; }

    @Override public void registerWebSocketHandlers(WebSocketHandlerRegistry r) {
        r.addHandler(handler, "/ws/drivers/*"); // Spring's same-origin default; production handshake is JWT-protected.
    }
}

@Component
class DriverSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(DriverSocketHandler.class);
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();   // connections held by THIS pod
    private final StringRedisTemplate redis;
    private final long leaseMillis;
    private final Counter connectionLimitRejected;
    private final Counter redisErrors;
    private static final DefaultRedisScript<Long> RELEASE_LEASE = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end", Long.class);
    private static final DefaultRedisScript<Long> RENEW_LEASE = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end", Long.class);

    DriverSocketHandler(StringRedisTemplate redis, MeterRegistry meters,
                        @Value("${WEBSOCKET_SESSION_LEASE_MS:30000}") long leaseMillis) {
        this.redis = redis;
        this.leaseMillis = Math.max(15000, leaseMillis);
        this.connectionLimitRejected = Counter.builder("ridehail.driver.websocket.rejected")
                .description("WebSocket handshakes rejected because this driver already has a live session")
                .register(meters);
        this.redisErrors = Counter.builder("ridehail.driver.websocket.redis.errors")
                .description("Redis errors while acquiring, renewing, or releasing WebSocket leases")
                .register(meters);
        Gauge.builder("ridehail.driver.websocket.active", sessions, Map::size)
                .description("Active driver WebSocket sessions on this pod")
                .register(meters);
    }

    @Override public void afterConnectionEstablished(WebSocketSession session) {
        String id = session.getPrincipal() == null ? driverId(session) : session.getPrincipal().getName();
        String requestedId = driverId(session);
        if (!id.equals(requestedId)) {
            try { session.close(CloseStatus.NOT_ACCEPTABLE.withReason("driver identity mismatch")); }
            catch (Exception ignored) { }
            return;
        }
        String leaseKey = leaseKey(id);
        try {
            Boolean acquired = redis.opsForValue().setIfAbsent(leaseKey, session.getId(), Duration.ofMillis(leaseMillis));
            if (!Boolean.TRUE.equals(acquired)) {
                connectionLimitRejected.increment();
                close(session, CloseStatus.POLICY_VIOLATION.withReason("one active WebSocket per driver"));
                return;
            }
        } catch (RuntimeException e) {
            redisErrors.increment();
            close(session, CloseStatus.SERVICE_RESTARTED.withReason("connection limiter unavailable"));
            return;
        }
        session.getAttributes().put("driverId", id);
        var safe = new ConcurrentWebSocketSessionDecorator(session, 5_000, 64 * 1024);  // sends may come from several threads
        WebSocketSession previous = sessions.put(id, safe);
        if (previous != null && previous.isOpen()) close(previous, CloseStatus.SESSION_NOT_RELIABLE);
        try {
            String pending = redis.opsForValue().get("driver:{" + id + "}:offer");      // reconnect => don't miss a live offer
            if (pending != null) send(safe, pending);
        } catch (RuntimeException e) {
            redisErrors.increment();
            sessions.remove(id, safe);
            releaseLease(id, session.getId());
            close(safe, CloseStatus.SERVICE_RESTARTED.withReason("connection limiter unavailable"));
        }
    }

    @Override public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String id = (String) session.getAttributes().get("driverId");
        if (id != null) sessions.computeIfPresent(id, (k, v) -> v.getId().equals(session.getId()) ? null : v);
        if (id != null) releaseLease(id, session.getId());
    }

    /** Refresh leases; a process crash frees them after the bounded lease TTL. */
    @Scheduled(fixedDelayString = "${WEBSOCKET_SESSION_RENEW_MS:10000}")
    void renewLeases() {
        for (Map.Entry<String, WebSocketSession> entry : sessions.entrySet()) {
            WebSocketSession session = entry.getValue();
            if (!session.isOpen()) {
                sessions.remove(entry.getKey(), session);
                releaseLease(entry.getKey(), session.getId());
                continue;
            }
            try {
                Long renewed = redis.execute(RENEW_LEASE, List.of(leaseKey(entry.getKey())),
                        session.getId(), Long.toString(leaseMillis));
                if (!Long.valueOf(1).equals(renewed)) {
                    sessions.remove(entry.getKey(), session);
                    close(session, CloseStatus.SERVICE_RESTARTED.withReason("connection lease expired"));
                }
            } catch (RuntimeException e) {
                redisErrors.increment();
                sessions.remove(entry.getKey(), session);
                close(session, CloseStatus.SERVICE_RESTARTED.withReason("connection limiter unavailable"));
            }
        }
    }

    @Override protected void handleTextMessage(WebSocketSession session, TextMessage m) {
        if ("ping".equals(m.getPayload())) send(session, "pong");                        // app-level keepalive through proxies
    }

    void push(String driverId, String json) {
        WebSocketSession s = sessions.get(driverId);
        if (s != null && s.isOpen()) send(s, json);
    }

    private void send(WebSocketSession s, String payload) {
        try { s.sendMessage(new TextMessage(payload)); } catch (Exception e) { log.warn("ws send failed: {}", e.toString()); }
    }
    private void releaseLease(String id, String sessionId) {
        try { redis.execute(RELEASE_LEASE, List.of(leaseKey(id)), sessionId); }
        catch (RuntimeException e) { redisErrors.increment(); }
    }
    private static String leaseKey(String driverId) {
        try {
            String hash = java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(driverId.getBytes(StandardCharsets.UTF_8)));
            return "ridehail:ws:driver:" + hash;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the runtime", e);
        }
    }
    private static void close(WebSocketSession session, CloseStatus status) {
        try { session.close(status); } catch (Exception ignored) { }
    }
    private static String driverId(WebSocketSession s) {
        String p = s.getUri().getPath();
        return p.substring(p.lastIndexOf('/') + 1);
    }
}

/**
 * Every pod must see every offer (the driver's socket may be on any pod), so each pod uses its own
 * consumer group and starts from "latest". Only pods that hold the driver's socket actually push.
 */
@Component
class OfferPushListener {
    private final DriverSocketHandler handler;
    OfferPushListener(DriverSocketHandler handler) { this.handler = handler; }

    @KafkaListener(topics = DRIVER_OFFERED, groupId = "driver-push-#{T(java.util.UUID).randomUUID()}", concurrency = "6",
                   properties = "auto.offset.reset=latest")
    void push(String msg) { handler.push(fromJson(msg, DriverOffered.class).driverId(), msg); }
}
