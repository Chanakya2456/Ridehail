create table trips (
    id uuid primary key,
    passenger_id varchar(255) not null,
    pickup_lat double precision not null,
    pickup_lng double precision not null,
    drop_lat double precision not null,
    drop_lng double precision not null,
    status varchar(32) not null,
    driver_id varchar(255),
    created_at timestamptz not null
);
create index trips_passenger_created_idx on trips(passenger_id, created_at desc);

create table outbox (
    id uuid primary key,
    topic varchar(255) not null,
    msg_key varchar(255) not null,
    payload text not null,
    created_at timestamptz not null,
    sent boolean not null default false
);
create index outbox_unsent_created_idx on outbox(created_at) where sent = false;
