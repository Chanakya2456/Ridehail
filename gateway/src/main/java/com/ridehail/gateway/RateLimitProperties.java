package com.ridehail.gateway;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ridehail.rate-limit")
record RateLimitProperties(Bucket caller, Bucket address, Map<String, RoutePolicy> routes) {
    record Bucket(int replenishRate, int burstCapacity) {}
    record RoutePolicy(Bucket caller, Bucket address) {}
}
