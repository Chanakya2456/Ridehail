package com.ridehail.gateway;

import java.security.Principal;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
class RateLimitConfig {
    /** Authenticated callers are limited by subject; local/demo callers fall back to remote address. */
    @Bean KeyResolver principalOrAddressKeyResolver() {
        return exchange -> exchange.getPrincipal().map(Principal::getName)
                .switchIfEmpty(Mono.fromSupplier(() -> exchange.getRequest().getRemoteAddress() == null
                        ? "unknown" : exchange.getRequest().getRemoteAddress().getHostString()));
    }
}
