alter table trips add column idempotency_key varchar(200);
create unique index trips_passenger_idempotency_uq
    on trips(passenger_id, idempotency_key)
    where idempotency_key is not null;
