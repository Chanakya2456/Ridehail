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

`kubectl apply -k k8s/` deploys the whole **single-node development stack** into the `ridehail` namespace: gateway, all four services, Keycloak with the demo realm, PostgreSQL, Redis, and Kafka. Postgres, Redis, and Kafka use StatefulSets with persistent volume claims. The API services run with JWT validation enabled. The bundled broker/database/provider use demo credentials and plaintext internal connections; this is not a production topology.

Requirements: a Kubernetes cluster with a default dynamic `StorageClass`, at least 6 GiB of memory available, Docker, and kubectl. For Docker Desktop Kubernetes, build images into its Docker engine. For Minikube or Kind, load the locally built images into that cluster after building:

```powershell
$modules = @('gateway', 'trip-service', 'location-service', 'dispatch-service', 'driver-service')
foreach ($module in $modules) {
  docker build --build-arg MODULE=$module -t "ridehail/${module}:1.0.0" .
}
# Minikube: minikube image load ridehail/gateway:1.0.0 (repeat for each image)
# Kind: kind load docker-image ridehail/gateway:1.0.0 (repeat for each image)
kubectl apply -k k8s/
kubectl get pods,pvc -n ridehail
```

Wait until all pods are Ready, then expose the internal services from two terminals:

```powershell
kubectl -n ridehail port-forward svc/gateway 8080:80
kubectl -n ridehail port-forward svc/keycloak 8180:8080
```

The gateway is at `http://localhost:8080`; Keycloak is at `http://localhost:8180` (admin / admin-demo-only). For a passenger token, run the token request shown in [Local OIDC test provider](#local-oidc-test-provider), changing the URL to `http://localhost:8180` if needed. Demo rider and driver IDs are `p1` and `d1`.

This manifest needs a cluster storage provisioner and built/pushed images; Kubernetes cannot create the cluster or a registry for you. It uses one Postgres pod, one Redis pod, and one Kafka broker without TLS, multiple replicas, or backup automation, so it provides no HA or disaster recovery. For production, use managed or properly clustered stateful services, external secret management, TLS, a real identity provider, ingress/WAF, network policies, PodDisruptionBudgets, backup/restore, and capacity-tested autoscaling. HPA is intentionally omitted because it requires a working metrics server and measured targets.

## Where the hard problems are handled
| Problem | Handling |
|---|---|
| Two trips grab one driver | Lua script: check busy + reserve-if-absent, atomic in Redis |
| Duplicate / redelivered events | Reservation is idempotent per tripId; trip and driver updates are state-guarded |
| Stale / out-of-order locations | Lua upsert rejects older `ts`; dispatch ignores drivers unseen >30s; scheduled atomic eviction >60s |
| Lost events (DB write then Kafka crash) | Transactional outbox in trip-service, `FOR UPDATE SKIP LOCKED` so pods can share the drain |
| No driver available | Dispatch retries with bounded exponential backoff; then publishes `dispatch.failed` as a business outcome |
| Transient listener failures | 5 retries, starting at 500ms and capped at 4s; then publish to `<topic>.DLT` and confirm broker acknowledgement |
| Bad payloads | Non-retryable → straight to DLT; malformed dispatch requests are not reported as no-driver outcomes |
| Horizontal scale | Six Kafka partitions and two listener consumers per Trip/Driver/Dispatch pod; topic replication is configurable |
| Crash after reserve, before assign | Reservation TTL (120s) frees the driver |
| Accept vs timeout race | Both are `offer.responded` events keyed by tripId (ordered); first one to change `trip:{id}:current` wins, the other is a no-op |
| Rejected driver re-offered same trip | Per-trip `excluded` set (its size also caps offers at 5) |
| Crash between offer state write and publish | Offer sits in `{offers}:pending`; sweeper turns it into a TIMEOUT and moves on |

## Security profiles

Compose sets `SPRING_PROFILES_ACTIVE=local`; this profile is for local development only and permits unauthenticated requests.
The `prod` profile validates JWT issuer and `JWT_AUDIENCE` at the gateway and each service. Tokens need `passenger` or `driver` scopes and a stable `sub` equal to the passenger/driver ID:
drivers can only update their own location, view their own profile/offers, and respond to their own offers; passengers can only view their own trips;
only the assigned driver can complete a trip. Monitoring scrapes need the `monitoring` scope. Configure the identity provider and claims before deployment.
WebSocket handshakes use same-origin defaults and must carry the driver's bearer token.
`POST /trips` accepts an `Idempotency-Key` up to 200 characters so client retries return the original trip.

The gateway enforces Redis-backed token buckets before proxying requests. A caller bucket (authenticated JWT subject, or source address for anonymous local requests) is shared across **all routes** at 10 requests/second with a burst of 30. A second shared source-address bucket allows 100/second with a burst of 300. Sensitive routes also have their own caller and/or address buckets: trip creation (1/second, burst 3; address 20/second, burst 40), location updates (2/second, burst 5; address 30/second, burst 60), driver registration (address 1/second, burst 3), offer polling (2/second, burst 5), and offer responses (1/second, burst 3). Tune these with the `RATE_LIMIT_*` environment variables. Gateway replicas share counters through Redis.

Rate-limit denials return HTTP 429, JSON code `rate_limited`, `Retry-After` (seconds), `X-RateLimit-Limit`, and `X-RateLimit-Remaining`. A Redis error fails closed with 503 and `Retry-After: 1`; the gateway records `ridehail.gateway.rate_limit.rejected` and `ridehail.gateway.rate_limit.redis.errors`. Each driver may hold one active WebSocket across all driver-service pods. A Redis lease renews every 10 seconds and expires after 30 seconds if its pod dies. Redis errors reject new handshakes or close existing sessions; driver-service records `ridehail.driver.websocket.active`, `ridehail.driver.websocket.rejected`, and `ridehail.driver.websocket.redis.errors`.

The source address is taken from the network connection. Behind an ingress/load balancer, this may identify the proxy and combine unrelated clients into one bucket. Production ingress must provide a trusted client address, and the gateway must be configured to trust only that ingress; do not trust arbitrary client-supplied forwarding headers. Restrict direct access to backend services so callers cannot bypass gateway limits. These are starter quotas, not measured capacity targets: tune them using traffic and 429 metrics before production. Apply the example Prometheus Operator alerts with `kubectl apply -f k8s/monitoring/ridehail-rate-limit-alerts.yaml`; this requires the Prometheus Operator CRD and a Prometheus instance configured to select the rule. Tune the included alert thresholds from observed traffic.

### Local OIDC test provider

To exercise the authenticated `prod` security profile locally, use the included Keycloak realm (demo credentials only):

```sh
docker compose -f docker-compose.yml -f docker-compose.auth.yml up --build
```

Keycloak is available at `http://localhost:8180` (admin / admin). The demo users are `p1` / `passenger-pass`, `d1` / `driver-pass`, and `monitor` / `monitor-pass`. Request a token:

```sh
curl -X POST http://localhost:8180/realms/ridehail/protocol/openid-connect/token \
  -H 'content-type: application/x-www-form-urlencoded' \
  -d 'grant_type=password&client_id=ridehail-api&client_secret=ridehail-local-client-secret&username=p1&password=passenger-pass&scope=openid'
```

Use the returned `access_token` as `Authorization: Bearer <token>` at `http://localhost:8080`. The demo issuer, credentials, client secret, and HTTP endpoints are for local development only.

PostgreSQL tables are created by Flyway and checked by Hibernate (`ddl-auto: validate`). Trip and driver services keep separate migration history tables.

## Production deployment requirements

The included `k8s/ridehail.yaml` deploys a complete development stack, not a production cluster. Replace its demo secret values and ConfigMap before adapting it for a real deployment.
Production profile defaults require TLS for PostgreSQL (`POSTGRES_SSL_MODE=verify-full`), Kafka (`KAFKA_SECURITY_PROTOCOL=SSL`) and Redis (`REDIS_SSL_ENABLED=true`).
Mount trusted CA certificates where the JVM needs them. For SASL Kafka, set `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and provide `KAFKA_SASL_MECHANISM` plus `KAFKA_SASL_JAAS_CONFIG` as a secret.
Provide TLS ingress, managed/high-availability Kafka, Redis and PostgreSQL, network policies, backups/restore procedures, and resource-specific alerts. The sample Compose dependencies are single-node development services.

Actuator Prometheus metrics and Kubernetes liveness/readiness probes are enabled. Kafka dead-letter recovery increments `ridehail.kafka.dlt.records` by service; DLT topics retain records for 14 days. Trip Service exports `ridehail.outbox.pending`.
Configure alerts for both, and maintain an operator-controlled DLT inspection/replay process. Remaining work before production includes broader cross-service failure tests,
distributed tracing, richer dispatch business metrics, and reconciliation for partial failures between Redis, Kafka and PostgreSQL. The dispatch consumer uses blocking retries,
which blocks that consumer during backoff; with two consumers per pod, a failed partition does not stall every partition assigned to that pod. Prometheus must scrape with a `monitoring` scope token.
