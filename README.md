# Ride-hailing dispatch backend

```
Driver → gateway → location-service → Redis GEO ({drivers}:geo, {drivers}:seen) → Kafka driver.location.updated
Rider  → gateway → trip-service → Postgres (trip + outbox, one tx) → Kafka trip.created
       → dispatch-service → Redis GEOSEARCH → Lua atomic reserve → Kafka driver.offered
       → driver-service (driver polls GET /drivers/{id}/offer) → accept | reject → Kafka offer.responded
       → dispatch-service:
            accepted → Kafka driver.assigned → trip-service (ASSIGNED) + driver-service (ON_TRIP)
            rejected / 20s timeout → release driver, exclude for this trip, offer next-nearest
            5 rejections or no drivers → Kafka dispatch.failed → trip-service (NO_DRIVER)
```

## Run locally
```
docker compose up --build
```

## Try it
```
curl -XPOST localhost:8080/drivers -H 'content-type: application/json' -d '{"id":"d1","name":"Asha"}'
curl -XPOST localhost:8080/drivers/d1/location -H 'content-type: application/json' -d '{"lat":19.0760,"lng":72.8777}'
curl -XPOST localhost:8080/trips -H 'content-type: application/json' -H 'X-Passenger-Id: p1' -H 'Idempotency-Key: demo-trip-001' \
  -d '{"pickupLat":19.0765,"pickupLng":72.8780,"dropLat":19.12,"dropLng":72.90}'
curl localhost:8080/drivers/d1/offer                 # driver app sees the offer (expires in 20s)
curl -XPOST localhost:8080/drivers/d1/offers/<tripId>/accept   # or /reject; ignoring it = timeout
curl localhost:8080/trips/<id>          # -> ASSIGNED, driverId=d1 (or NO_DRIVER)
curl -XPATCH localhost:8080/trips/<id>/complete   # frees the driver
```
Keep sending driver locations (every 3-5s); drivers silent for >30s are not dispatched, >60s are evicted.

## Push offers over WebSocket
```
websocat ws://localhost:8080/ws/drivers/d1      # receives DriverOffered JSON the moment dispatch offers a trip
curl -XPOST localhost:8080/drivers/d1/offers/<tripId>/accept
```
Send the text `ping` to get `pong` (keepalive). Reconnecting mid-offer re-sends the live offer. Each driver-service pod uses its own
Kafka consumer group so whichever pod holds the socket sees the offer; no sticky sessions needed. `GET /drivers/{id}/offer` still works as a fallback.

## Tests
`mvn test` — includes the Dispatch Kafka/Redis integration flow (Testcontainers, needs Docker) and unit tests for trip idempotency and driver ownership. The dispatch scenarios cover:
reject → next driver, timeout → next driver, no drivers → `dispatch.failed`, duplicate `trip.created`, two trips racing for one driver.

## Kubernetes
```
for m in gateway trip-service location-service dispatch-service driver-service; do
  docker build --build-arg MODULE=$m -t ridehail/$m:1.0.0 . ; done
# Kafka/Redis/Postgres: managed services or Strimzi / Bitnami charts; adjust k8s/ridehail.yaml ConfigMap hosts
kubectl apply -f k8s/ridehail.yaml
```

## Where the hard problems are handled
| Problem | Handling |
|---|---|
| Two trips grab one driver | Lua script: check busy + reserve-if-absent, atomic in Redis |
| Duplicate / redelivered events | Reservation is idempotent per tripId; trip and driver updates are state-guarded |
| Stale / out-of-order locations | Lua upsert rejects older `ts`; dispatch ignores drivers unseen >30s; scheduled atomic eviction >60s |
| Lost events (DB write then Kafka crash) | Transactional outbox in trip-service, `FOR UPDATE SKIP LOCKED` so pods can share the drain |
| No driver available / transient failures | Listener throws → 10 retries × 2s → `<topic>.DLT` |
| Bad payloads | Non-retryable → straight to DLT |
| Horizontal scale | Stateless services; topics have 6 partitions keyed by trip/driver id; HPA per service (dispatch max 6) |
| Crash after reserve, before assign | Reservation TTL (120s) frees the driver |
| Accept vs timeout race | Both are `offer.responded` events keyed by tripId (ordered); first one to change `trip:{id}:current` wins, the other is a no-op |
| Rejected driver re-offered same trip | Per-trip `excluded` set (its size also caps offers at 5) |
| Crash between offer state write and publish | Offer sits in `{offers}:pending`; sweeper turns it into a TIMEOUT and moves on |

## Security profiles

Compose sets `SPRING_PROFILES_ACTIVE=local`; this profile is for local development only and permits unauthenticated requests.
The `prod` profile validates JWT issuer and `JWT_AUDIENCE` at the gateway and each service. Tokens need `passenger` or `driver` scopes and a stable `sub` equal to the passenger/driver ID:
drivers can only update their own location, view their own profile/offers, and respond to their own offers; passengers can only view their own trips;
only the assigned driver can complete a trip. Monitoring scrapes need the `monitoring` scope. Configure the identity provider and claims before deployment.
WebSocket handshakes use same-origin defaults and must carry the driver's bearer token. The gateway rate-limits by token subject (or source address locally): 10 requests/second, burst 30.
`POST /trips` accepts an `Idempotency-Key` up to 200 characters so client retries return the original trip.

PostgreSQL tables are created by Flyway and checked by Hibernate (`ddl-auto: validate`). Trip and driver services keep separate migration history tables.

## Production deployment requirements

`k8s/ridehail.yaml` is a starting template, not a turnkey production cluster. Replace the example JWT issuer and create the referenced `ridehail-secret` through your cluster secret manager with `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD`, and `REDIS_PASSWORD`.
Production profile defaults require TLS for PostgreSQL (`POSTGRES_SSL_MODE=verify-full`), Kafka (`KAFKA_SECURITY_PROTOCOL=SSL`) and Redis (`REDIS_SSL_ENABLED=true`).
Mount trusted CA certificates where the JVM needs them. For SASL Kafka, set `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and provide `KAFKA_SASL_MECHANISM` plus `KAFKA_SASL_JAAS_CONFIG` as a secret.
Provide TLS ingress, managed/high-availability Kafka, Redis and PostgreSQL, network policies, backups/restore procedures, and resource-specific alerts. The sample Compose dependencies are single-node development services.

Actuator Prometheus metrics and Kubernetes liveness/readiness probes are enabled. Kafka dead-letter recovery increments `ridehail.kafka.dlt.records` by service; Trip Service exports `ridehail.outbox.pending`.
Configure alerts for both, and maintain an operator-controlled DLT inspection/replay process. Remaining work before production includes broader cross-service failure tests,
distributed tracing, richer dispatch business metrics, and reconciliation for partial failures between Redis, Kafka and PostgreSQL. The dispatch consumer uses blocking retries,
which hold its partition during backoff. Prometheus must scrape with a `monitoring` scope token.
