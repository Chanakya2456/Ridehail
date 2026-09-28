package com.ridehail.gateway;

import java.security.Principal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
class RateLimitConfig {
    /** Per-caller key; only a fixed-length digest is stored in Redis. */
    @Bean KeyResolver principalOrAddressKeyResolver() {
        return exchange -> exchange.getPrincipal().map(Principal::getName)
                .map(name -> digest("subject:" + name))
                .switchIfEmpty(Mono.fromSupplier(() -> digest("address:" + remoteAddress(exchange))));
    }

    /** Independent source-address bucket prevents one token from bypassing an IP guardrail. */
    @Bean KeyResolver remoteAddressKeyResolver() {
        return exchange -> Mono.just(digest("address:" + remoteAddress(exchange)));
    }

    private static String remoteAddress(org.springframework.web.server.ServerWebExchange exchange) {
        var address = exchange.getRequest().getRemoteAddress();
        return address == null ? "unknown" : address.getAddress() == null
                ? address.getHostString() : address.getAddress().getHostAddress();
    }

    private static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the runtime", e);
        }
    }
}
