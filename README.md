# rnvgtfsproxy

Simple Go service that polls the RNV GTFS-Realtime feeds, caches them in memory, and exposes them via HTTP.
Currently, one needs to authenticate, and this project does the authentification and provides a simple HTTP endpoint for all the RT streams.

## Features
- Polls GTFS-RT feeds every 30s
- In-memory caching
- OAuth2 client credentials auth
- JSON + protobuf endpoints
- Entity count + feed stats
- Graceful shutdown

## Endpoints
- `/tripupdates.pb`
- `/alerts.pb`
- `/tripupdates.json`
- `/alerts.json`
- `/status`
- `/healthz`


## Setup

Create an `.env` file:

```bash
TENANT_ID=...
CLIENT_ID=...
CLIENT_SECRET=...
RESOURCE=...
HOSTNAME=https://your-gtfs-endpoint
POLL_INTERVAL=30
PORT=8000
```

Build the binary `go build -o gtfs-proxy`, and then run it `./gtfs-proxy`.
