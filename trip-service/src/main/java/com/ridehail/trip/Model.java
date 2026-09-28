package com.ridehail.trip;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

@Entity @Table(name = "trips")
class Trip {
    enum Status { REQUESTED, ASSIGNED, COMPLETED, NO_DRIVER }
    @Id public UUID id;
    public String passengerId;
    @Column(name = "idempotency_key", length = 200) public String idempotencyKey;
    public double pickupLat, pickupLng, dropLat, dropLng;
    @Enumerated(EnumType.STRING) public Status status;
    public String driverId;
    public Instant createdAt;
}

/** Transactional outbox: event is written in the same DB tx as the trip, published later. */
@Entity @Table(name = "outbox")
class OutboxEvent {
    @Id public UUID id;
    public String topic, msgKey;
    @Column(columnDefinition = "text") public String payload;
    public Instant createdAt;
    public boolean sent;
}

interface TripRepo extends JpaRepository<Trip, UUID> {
    java.util.Optional<Trip> findByPassengerIdAndIdempotencyKey(String passengerId, String idempotencyKey);
}

interface OutboxRepo extends JpaRepository<OutboxEvent, UUID> {
    long countBySentFalse();

    // SKIP LOCKED => several trip-service pods can drain the outbox without double-sending
    @Query(value = "select * from outbox where sent = false order by created_at limit 100 for update skip locked",
           nativeQuery = true)
    List<OutboxEvent> lockBatch();
}
