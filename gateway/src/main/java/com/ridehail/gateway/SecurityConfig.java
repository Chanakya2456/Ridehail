package com.ridehail.gateway;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import com.ridehail.common.JwtDecoderFactory;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@Configuration
@Profile("prod")
class SecurityConfig {
    @Bean ReactiveJwtDecoder jwtDecoder(@Value("${JWT_ISSUER_URI}") String issuer,
                                         @Value("${JWT_AUDIENCE}") String audience) {
        JwtDecoder delegate = JwtDecoderFactory.forApi(issuer, audience);
        return token -> Mono.fromCallable(() -> delegate.decode(token)).subscribeOn(Schedulers.boundedElastic());
    }

    @Bean SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http.csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/actuator/health/**").permitAll()
                        .pathMatchers("/actuator/prometheus").hasAuthority("SCOPE_monitoring")
                        .anyExchange().authenticated())
                .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> {})).build();
    }
}
