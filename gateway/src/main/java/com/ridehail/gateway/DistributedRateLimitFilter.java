package com.ridehail.gateway;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Mono;

/**
 * Atomic distributed token buckets with one caller quota shared by all routes.
 * Redis errors fail closed with 503 so a Redis outage cannot silently disable
 * request protection.
 */
@Component
@EnableConfigurationProperties(RateLimitProperties.class)
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
class DistributedRateLimitFilter implements GlobalFilter, WebFilter, Ordered {
    private static final DefaultRedisScript<String> TOKEN_BUCKET = new DefaultRedisScript<>();

    static {
        TOKEN_BUCKET.setScriptText("""
                local t = redis.call('TIME')
                local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
                local rate = tonumber(ARGV[1])
                local capacity = tonumber(ARGV[2])
                local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
                local last = tonumber(redis.call('HGET', KEYS[1], 'last'))
                if tokens == nil then tokens = capacity end
                if last == nil then last = now end
                tokens = math.min(capacity, tokens + math.max(0, now - last) * rate / 1000)
                local allowed = 0
                local retry = 0
                if tokens >= 1 then
                  tokens = tokens - 1
                  allowed = 1
                else
                  retry = math.ceil((1 - tokens) * 1000 / rate)
                end
                redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'last', tostring(now))
                redis.call('PEXPIRE', KEYS[1], math.max(1000, math.ceil(capacity * 2000 / rate)))
                return tostring(allowed) .. ':' .. tostring(math.floor(tokens)) .. ':' .. tostring(retry)
                """);
        TOKEN_BUCKET.setResultType(String.class);
    }

    private final ReactiveStringRedisTemplate redis;
    private final RateLimitProperties properties;
    private final MeterRegistry meters;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    DistributedRateLimitFilter(ReactiveStringRedisTemplate redis, RateLimitProperties properties, MeterRegistry meters) {
        this.redis = redis;
        this.properties = properties;
        this.meters = meters;
    }

    @Override public int getOrder() { return Ordered.HIGHEST_PRECEDENCE + 10; }

    /** Apply the shared address guard before authentication, including rejected/invalid-token requests. */
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (path.startsWith("/actuator/")) return chain.filter(exchange);
        Check edge = new Check("edge_address", "ridehail:rl:global:address:" + RateLimitConfig.addressKey(exchange),
                properties.address());
        return runBucket(edge)
                .onErrorMap(RateLimitUnavailableException::new)
                .flatMap(result -> {
                    if (result.allowed()) return chain.filter(exchange);
                    increment("ridehail.gateway.rate_limit.rejected", "policy", result.policy(), "route", "pre_auth");
                    return reject(exchange, result);
                })
                .onErrorResume(RateLimitUnavailableException.class, error -> {
                    increment("ridehail.gateway.rate_limit.redis.errors", "route", "pre_auth");
                    exchange.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
                    exchange.getResponse().getHeaders().set(HttpHeaders.RETRY_AFTER, "1");
                    return exchange.getResponse().setComplete();
                });
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, org.springframework.cloud.gateway.filter.GatewayFilterChain chain) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route == null) return chain.filter(exchange);

        return exchange.getPrincipal()
                .map(principal -> (Principal) principal)
                .defaultIfEmpty(new Principal() {
                    @Override public String getName() { return ""; }
                })
                .flatMap(principal -> enforce(exchange, route, principal, chain));
    }

    private Mono<Void> enforce(ServerWebExchange exchange, Route route, Principal principal,
                               org.springframework.cloud.gateway.filter.GatewayFilterChain chain) {
        String caller = RateLimitConfig.callerKey(principal.getName().isEmpty() ? null : principal, exchange);
        String address = RateLimitConfig.addressKey(exchange);
        List<Check> checks = new ArrayList<>();
        checks.add(new Check("global_caller", "ridehail:rl:global:caller:" + caller, properties.caller()));

        RateLimitProperties.RoutePolicy routePolicy = properties.routes().get(route.getId());
        if (routePolicy != null) {
            if (routePolicy.caller() != null) {
                checks.add(new Check("route_caller", "ridehail:rl:route:" + route.getId() + ":caller:" + caller,
                        routePolicy.caller()));
            }
            if (routePolicy.address() != null) {
                checks.add(new Check("route_address", "ridehail:rl:route:" + route.getId() + ":address:" + address,
                        routePolicy.address()));
            }
        }

        return check(checks, 0, null)
                .onErrorMap(RateLimitUnavailableException::new)
                .flatMap(result -> {
                    if (result.allowed()) return chain.filter(exchange);
                    increment("ridehail.gateway.rate_limit.rejected", "policy", result.policy(), "route", route.getId());
                    return reject(exchange, result);
                })
                .onErrorResume(RateLimitUnavailableException.class, error -> {
                    increment("ridehail.gateway.rate_limit.redis.errors", "route", route.getId());
                    exchange.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
                    exchange.getResponse().getHeaders().set(HttpHeaders.RETRY_AFTER, "1");
                    return exchange.getResponse().setComplete();
                });
    }

    private Mono<Void> reject(ServerWebExchange exchange, Decision result) {
        long retrySeconds = Math.max(1, (result.retryAfterMillis() + 999) / 1000);
        HttpHeaders headers = exchange.getResponse().getHeaders();
        headers.set("X-RateLimit-Limit", Integer.toString(result.bucket().replenishRate()));
        headers.set("X-RateLimit-Remaining", Integer.toString(result.remaining()));
        headers.set(HttpHeaders.RETRY_AFTER, Long.toString(retrySeconds));
        headers.setContentType(MediaType.APPLICATION_JSON);
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        byte[] body = ("{\"error\":\"rate_limited\",\"message\":\"Request quota exceeded\",\"retryAfterSeconds\":" + retrySeconds + "}")
                .getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    private Mono<Decision> check(List<Check> checks, int index, Decision last) {
        if (index >= checks.size()) return Mono.just(last);
        Check item = checks.get(index);
        if (item.bucket() == null || item.bucket().replenishRate() <= 0 || item.bucket().burstCapacity() <= 0) {
            return check(checks, index + 1, last);
        }
        return runBucket(item).flatMap(decision -> decision.allowed()
                ? check(checks, index + 1, decision)
                : Mono.just(decision));
    }

    private Mono<Decision> runBucket(Check item) {
        return redis.execute(TOKEN_BUCKET, List.of(item.key()),
                        Integer.toString(item.bucket().replenishRate()), Integer.toString(item.bucket().burstCapacity()))
                .next()
                .switchIfEmpty(Mono.error(new IllegalStateException("Redis rate-limit script returned no result")))
                .map(raw -> parse(raw, item));
    }

    private Decision parse(String raw, Check item) {
        String[] values = raw.split(":");
        if (values.length != 3) throw new IllegalStateException("Invalid rate-limit result from Redis");
        return new Decision("1".equals(values[0]), Integer.parseInt(values[1]), Long.parseLong(values[2]),
                item.policy(), item.bucket());
    }

    private void increment(String metric, String... tags) {
        String key = metric + java.util.Arrays.toString(tags);
        counters.computeIfAbsent(key, ignored -> Counter.builder(metric)
                .tags(tags)
                .register(meters)).increment();
    }

    private record Check(String policy, String key, RateLimitProperties.Bucket bucket) {}
    private record Decision(boolean allowed, int remaining, long retryAfterMillis, String policy,
                            RateLimitProperties.Bucket bucket) {}
    private static final class RateLimitUnavailableException extends RuntimeException {
        RateLimitUnavailableException(Throwable cause) { super(cause); }
    }
}
