# rnvgtfsproxy

A lightweight Go proxy for the [RNV](https://www.rnv-online.de/) (Rhein-Neckar-Verbund) GTFS-Realtime feeds. RNV is the public transit authority covering the Rhine-Neckar metropolitan region in Germany, operating buses, trams, and light rail across Mannheim, Heidelberg, Ludwigshafen, and surrounding cities.

The RNV real-time API requires OAuth2 client credentials authentication. This proxy handles the auth, polls the upstream GTFS-RT feeds on a configurable interval, caches them in memory, and re-exposes them over plain HTTP — so any GTFS-RT consumer can reach them without needing credentials.

---

## How it works

```
RNV API (OAuth2)          rnvgtfsproxy              Your app
─────────────────         ────────────              ──────────
/tripupdates    ──poll──▶  in-memory cache  ──GET──▶  /tripupdates.pb
/alerts                    ETag + gzip               /alerts.pb
/tripupdates/decoded       every N seconds           /tripupdates.json
/alerts/decoded                                      /alerts.json
```

On startup the proxy fetches all feeds immediately, then re-fetches on every poll interval. Feeds are stored compressed; responses are served directly from the pre-compressed cache with proper HTTP caching headers.

---

## Endpoints

| Endpoint | Content-Type | Description |
|---|---|---|
| `GET /tripupdates.pb` | `application/octet-stream` | Trip updates as GTFS-RT protobuf binary |
| `GET /alerts.pb` | `application/octet-stream` | Service alerts as GTFS-RT protobuf binary |
| `GET /tripupdates.json` | `application/json` | Trip updates decoded to JSON |
| `GET /alerts.json` | `application/json` | Service alerts decoded to JSON |
| `GET /status` | `application/json` | Per-feed stats: last update time, ETag, size, entity count |
| `GET /healthz` | `text/plain` | `200 ok` when all feeds are fresh; `503` when not yet populated or stale |

### Caching headers

All feed endpoints return:

- `ETag` — SHA1 of the response body; clients can use `If-None-Match` for conditional requests
- `Last-Modified` — timestamp of the last successful upstream fetch; supports `If-Modified-Since`
- `Cache-Control: public, max-age=5, stale-while-revalidate=<interval-5>`
- `Vary: Accept-Encoding`
- `Content-Encoding: gzip` when the client sends `Accept-Encoding: gzip`

### `/status` example response

```json
{
  "tripupdates.json": {
    "last_update": "2024-01-15T14:23:01Z",
    "etag": "\"a3f1c2b9...\"",
    "size_bytes": 48210,
    "gzip_bytes": 9843,
    "entities": 312,
    "stale": false
  },
  "alerts.pb": {
    "last_update": "2024-01-15T14:23:01Z",
    "etag": "\"7e2d4f1a...\"",
    "size_bytes": 1204,
    "gzip_bytes": 610,
    "entities": -1,
    "stale": false
  }
}
```

`entities` is the count of GTFS-RT `entity` objects for JSON feeds, or `-1` for protobuf feeds. `stale` is `true` if the last successful fetch was more than 2 minutes ago.

### `/healthz` behaviour

| Condition | Status |
|---|---|
| Startup, feeds not yet populated | `503 not ready: feeds not yet populated` |
| Any feed last updated > 2 minutes ago | `503 not ready: feed "x" is stale` |
| All feeds fresh | `200 ok` |

Use `/healthz` as a Kubernetes readiness and liveness probe.

---

## Setup

### Prerequisites

- Go 1.22 or later
- RNV API credentials (see [RNV Developer Portal](https://www.rnv-online.de/))

### Configuration

Create a `.env` file in the project root (or export the variables directly):

```env
TENANT_ID=<Azure AD tenant ID>
CLIENT_ID=<Azure AD application client ID>
CLIENT_SECRET=<Azure AD application client secret>
RESOURCE=<API resource URI provided by RNV>
HOSTNAME=https://<rnv-gtfs-api-host>
POLL_INTERVAL=30      # seconds between upstream fetches (default: 30)
PORT=8000             # port to listen on (default: 8000)
```

All six of `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, `RESOURCE`, and `HOSTNAME` are required. The service will refuse to start if any are missing.

### Build and run

```bash
go build -o gtfs-proxy .
./gtfs-proxy
```

Or in one step:

```bash
go run .
```

### Docker

```dockerfile
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o gtfs-proxy .

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/gtfs-proxy .
EXPOSE 8000
ENTRYPOINT ["./gtfs-proxy"]
```

```bash
docker build -t rnvgtfsproxy .
docker run --env-file .env -p 8000:8000 rnvgtfsproxy
```

---

## GTFS-RT

The proxy serves feeds conforming to the [GTFS Realtime specification](https://gtfs.org/realtime/reference/). Protobuf endpoints (`.pb`) are the canonical binary format; JSON endpoints (`.json`) are the same data decoded for easier inspection and debugging.

Useful tools for working with the feeds:

- [`gtfs-realtime-bindings`](https://github.com/MobilityData/gtfs-realtime-bindings) — official language bindings for decoding protobuf
- [`proto-lens`](https://github.com/google/proto-lens) — CLI for inspecting `.pb` files
- `curl -s http://localhost:8000/tripupdates.json | jq '.entity | length'` — quick entity count

---

## Licence

MIT — see [LICENCE](LICENCE).
