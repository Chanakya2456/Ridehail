package com.ridehail.trip;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TripIdempotencyTest {
    @Test void retryWithSamePassengerAndKeyReturnsOriginalTripWithoutAnotherOutboxEvent() {
        TripRepo trips = Mockito.mock(TripRepo.class);
        OutboxRepo outbox = Mockito.mock(OutboxRepo.class);
        Trip prior = new Trip(); prior.id = UUID.randomUUID(); prior.passengerId = "p1";
        when(trips.findByPassengerIdAndIdempotencyKey("p1", "req-1")).thenReturn(Optional.of(prior));

        Trip result = new TripService(trips, outbox).create(request(), "p1", "req-1");

        assertThat(result).isSameAs(prior);
        verify(trips, never()).save(any());
        verify(outbox, never()).save(any());
    }

    @Test void newIdempotencyKeyPersistsTripAndTripCreatedOutboxTogether() {
        TripRepo trips = Mockito.mock(TripRepo.class);
        OutboxRepo outbox = Mockito.mock(OutboxRepo.class);
        when(trips.findByPassengerIdAndIdempotencyKey("p1", "req-2")).thenReturn(Optional.empty());

        Trip created = new TripService(trips, outbox).create(request(), "p1", "req-2");

        assertThat(created.id).isNotNull();
        assertThat(created.passengerId).isEqualTo("p1");
        verify(trips).save(created);
        verify(outbox).save(any(OutboxEvent.class));
    }

    private static TripController.CreateTrip request() {
        return new TripController.CreateTrip(19.0, 72.0, 19.1, 72.1);
    }
}
