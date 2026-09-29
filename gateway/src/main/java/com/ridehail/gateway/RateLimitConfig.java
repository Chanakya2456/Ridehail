package com.ridehail.gateway;

import java.security.Principal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.web.server.ServerWebExchange;

final class RateLimitConfig {
    private RateLimitConfig() {}

    static String callerKey(Principal principal, ServerWebExchange exchange) {
        return principal == null
                ? digest("address:" + remoteAddress(exchange))
                : digest("subject:" + principal.getName());
    }

    static String addressKey(ServerWebExchange exchange) {
        return digest("address:" + remoteAddress(exchange));
    }

    static String remoteAddress(ServerWebExchange exchange) {
        var address = exchange.getRequest().getRemoteAddress();
        return address == null ? "unknown" : address.getAddress() == null
                ? address.getHostString() : address.getAddress().getHostAddress();
    }

    static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the runtime", e);
        }
    }
}
