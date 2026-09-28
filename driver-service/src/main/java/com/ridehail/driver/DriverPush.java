package com.ridehail.driver;

import static com.ridehail.common.Events.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.config.annotation.*;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/** ws://gateway/ws/drivers/{driverId}  — server pushes DriverOffered JSON; driver answers via the REST accept/reject. */
@Configuration
@EnableWebSocket
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
    DriverSocketHandler(StringRedisTemplate redis) { this.redis = redis; }

    @Override public void afterConnectionEstablished(WebSocketSession session) {
        String id = session.getPrincipal() == null ? driverId(session) : session.getPrincipal().getName();
        String requestedId = driverId(session);
        if (!id.equals(requestedId)) {
            try { session.close(CloseStatus.NOT_ACCEPTABLE.withReason("driver identity mismatch")); }
            catch (Exception ignored) { }
            return;
        }
        session.getAttributes().put("driverId", id);
        var safe = new ConcurrentWebSocketSessionDecorator(session, 5_000, 64 * 1024);  // sends may come from several threads
        sessions.put(id, safe);
        String pending = redis.opsForValue().get("driver:{" + id + "}:offer");          // reconnect => don't miss a live offer
        if (pending != null) send(safe, pending);
    }

    @Override public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String id = (String) session.getAttributes().get("driverId");
        if (id != null) sessions.computeIfPresent(id, (k, v) -> v.getId().equals(session.getId()) ? null : v);
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
