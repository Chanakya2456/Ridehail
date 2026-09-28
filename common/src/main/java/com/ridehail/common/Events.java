package com.ridehail.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;

/** Kafka topics + event payloads shared by all services. */
public final class Events {
    public static final String DRIVER_LOCATION_UPDATED = "driver.location.updated";
    public static final String TRIP_CREATED = "trip.created";
    public static final String DRIVER_ASSIGNED = "driver.assigned";
    public static final String TRIP_COMPLETED = "trip.completed";
    public static final String DRIVER_OFFERED = "driver.offered";
    public static final String OFFER_RESPONDED = "offer.responded";
    public static final String DISPATCH_FAILED = "dispatch.failed";

    public record DriverLocationUpdated(String driverId, double lat, double lng, long ts) {}
    public record TripCreated(UUID tripId, double pickupLat, double pickupLng, long ts) {}
    public record DriverAssigned(UUID tripId, String driverId, long ts) {}
    public record TripCompleted(UUID tripId, String driverId, long ts) {}
    public record DriverOffered(UUID tripId, String driverId, double pickupLat, double pickupLng, long expiresAt, long ts) {}
    public record OfferResponded(UUID tripId, String driverId, boolean accepted, String reason, long ts) {}
    public record DispatchFailed(UUID tripId, String reason, long ts) {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String toJson(Object o) {
        try { return MAPPER.writeValueAsString(o); } catch (Exception e) { throw new IllegalStateException(e); }
    }
    /** Throws IllegalArgumentException on bad payloads -> treated as non-retryable (goes to DLT). */
    public static <T> T fromJson(String s, Class<T> c) {
        try { return MAPPER.readValue(s, c); } catch (Exception e) { throw new IllegalArgumentException("bad payload", e); }
    }
    private Events() {}
}
