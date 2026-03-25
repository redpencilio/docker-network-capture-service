# docker-network-capture-service

A [semantic.works](https://semantic.works) service that automatically attaches network traffic monitoring containers (packetbeat) to running Docker containers. Container state is tracked via a shared triplestore, populated by [docker-monitor-service](https://github.com/redpencilio/docker-monitor-service).

## How it works

The service reacts to two triggers:

- **Delta notifications** from the triplestore: when a container's status changes to `running`, a monitor is created; when it stops, the monitor is removed.
- **Periodic sync** (configurable interval): reconciles triplestore state with running monitors, restarting any that have crashed.

Monitor containers share the network namespace of their target (`NetworkMode: container:<id>`) and use `NET_ADMIN`/`NET_RAW` capabilities to capture HTTP traffic, which is forwarded to a logstash instance.

## Setup

This service is part of the [app-http-logger](https://github.com/redpencilio/app-http-logger) stack. Refer to that repo for full docker-compose configuration.

To select which containers to monitor, add the `logging` label to them:

```yaml
services:
  your-service:
    labels:
      - "logging=true"
```

The `CAPTURE_CONTAINER_FILTER` environment variable controls this via a SPARQL fragment — see configuration below.

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `CAPTURE_DOCKER_SOCKET` | `/var/run/docker.sock` | Path to the Docker socket |
| `CAPTURE_SYNC_INTERVAL` | `2500` | Interval (ms) between reconciliation syncs |
| `CAPTURE_CONTAINER_FILTER` | | SPARQL fragment to filter which containers to monitor. The container URI is bound as `?uri`. |
| `MONITOR_IMAGE` | `redpencil/http-logger-packetbeat-service` | Image used for monitor containers |
| `LOGSTASH_NETWORK` | | Docker network name used to reach logstash |
| `PACKETBEAT_MAX_MESSAGE_SIZE` | | Maximum captured message size in bytes |
| `PACKETBEAT_LISTEN_PORTS` | | YAML array of ports to capture traffic on, e.g. `[80, 8080]` |
| `MU_SPARQL_ENDPOINT` | `http://database:8890/sparql` | SPARQL endpoint |
| `MU_APPLICATION_GRAPH` | | Named graph to query container state from |
| `LOG_SPARQL_ALL` | `true` | Set to `false` to suppress SPARQL query logging |

## Delta notifications

The service listens for delta notifications at `POST /.mu/delta`. Configure the [mu-delta-notifier](https://github.com/mu-semtech/mu-delta-notifier) to send updates for `docker:status` changes.

Example delta notifier rule:

```javascript
{
  match: {
    predicate: { type: "uri", value: "https://w3.org/ns/bde/docker#status" }
  },
  callback: {
    url: "http://capture/.mu/delta",
    method: "POST"
  },
  options: { resourceFormat: "v0.0.1", gracePeriod: 250, ignoreFromSelf: false }
}
```

## Graceful shutdown

On `SIGTERM`/`SIGINT` the service removes all active monitor containers before exiting, ensuring no stale monitors are left behind when the stack is stopped.
