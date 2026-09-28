package com.ridehail.driver;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import com.ridehail.common.JwtDecoderFactory;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@Profile("prod")
class SecurityConfig {
    @Bean JwtDecoder jwtDecoder(@Value("${JWT_ISSUER_URI}") String issuer,
                                @Value("${JWT_AUDIENCE}") String audience) {
        return JwtDecoderFactory.forApi(issuer, audience);
    }

    @Bean SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http.csrf(csrf -> csrf.disable()).authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**").permitAll()
                        .requestMatchers("/actuator/prometheus").hasAuthority("SCOPE_monitoring")
                        .requestMatchers("/drivers/**", "/ws/drivers/**").hasAuthority("SCOPE_driver")
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth -> oauth.jwt(Customizer.withDefaults())).build();
    }
}
