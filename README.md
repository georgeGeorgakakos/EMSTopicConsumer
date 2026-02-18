# OptimusDB EMS Topic Consumer

A standalone STOMP client for consuming and producing messages on the OptimusDB EMS broker (ActiveMQ / TIBCO EMS). Designed to work alongside the [OptimusDB](https://github.com/georgegeorgakakos/optimusdb) distributed database cluster.

## Overview

OptimusDB uses an EMS (Enterprise Messaging Service) broker for event-driven communication between nodes and external systems. Messages are published to STOMP topics and consumed by OptimusDB agents via the `ems_subscriber.go` module.

This client provides a standalone tool to:

- **Monitor** all EMS traffic in real-time with color-coded, structured output
- **Inspect** STOMP headers and message bodies (JSON or Java-style key=value)
- **Send** messages to any topic to trigger OptimusDB actions
- **Log** all messages to JSONL files for offline analysis
- **Filter** by topic substring to focus on specific event streams
- **Debug** message routing, header propagation, and broker connectivity

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    EMS Broker                            │
│              (ActiveMQ / TIBCO EMS)                      │
│                  STOMP :61610                            │
│                                                         │
│  /topic/response_time_SENSOR                            │
│  /topic/cpu_util_instance                               │
│  /topic/optimusdb.commands                              │
│  /topic/optimusdb.events                                │
│  /topic/>  (wildcard — all topics)                      │
└────────┬──────────────┬─────────────────┬───────────────┘
         │              │                 │
         ▼              ▼                 ▼
   ┌──────────┐  ┌──────────┐     ┌──────────────┐
   │OptimusDB │  │OptimusDB │     │ EMS Client   │
   │ Node 1   │  │ Node 2   │     │ (this tool)  │
   │          │  │          │     │              │
   │ Go STOMP │  │ Go STOMP │     │ Python STOMP │
   │subscriber│  │subscriber│     │ consumer     │
   └──────────┘  └──────────┘     └──────────────┘
```

## Message Formats

The client handles two distinct message formats found in the OptimusDB ecosystem:

### 1. Body-Based JSON (OptimusDB Commands/Events)

Standard OptimusDB messages carry data in the STOMP body as JSON:

```json
{
  "action": "CREATE",
  "resource": "dataset",
  "params": {
    "name": "energy_metrics_2025",
    "owner": "george",
    "schema": "renewable"
  }
}
```

The Go-side handler (`ems_subscriber.go → handleEMSMessage`) parses this body and routes it to `ProcessEMS()`.

### 2. Header-Based (SwarmChestrate Monitoring/SENSOR)

Monitoring and telemetry messages from the SwarmChestrate platform carry all data in STOMP headers with an empty body:

```
Headers:
  destination:            /topic/response_time_SENSOR
  node-id:               da957267-058e-4e18-a4d5-3248db169c98
  timestamp:             1771447990930
  producer-host:         192.168.0.220
  instance:              192.168.0.220
  source-node:           10.42.0.11
  source-endpoint:       http://10.42.0.11:9000/
  metric:                request_processing_seconds_sum
  cloud:                 ${provider}
  region:                ${zone-id}
  zone:                  ${zone-id}
  priority:              4
  expires:               0

Body: (empty)
```

### Header Reference

| Header | Group | Description | Example |
|--------|-------|-------------|---------|
| `message-id` | Identity | Unique broker-assigned message ID | `ID:swch-monitoring-testbed-1-37457-...` |
| `node-id` | Identity | UUID of the producing node/agent | `da957267-058e-4e18-a4d5-3248db169c98` |
| `timestamp` | Identity | Unix epoch in milliseconds | `1771447990930` |
| `destination` | Routing | Topic the message was delivered on | `/topic/response_time_SENSOR` |
| `destination-topic` | Routing | Short topic name (no prefix) | `response_time_SENSOR` |
| `effective-destination` | Routing | Resolved destination after broker rules | `cpu_util_instance_SENSOR` |
| `original-destination` | Routing | Where the producer originally sent it | `cpu_util_instance_SENSOR` |
| `producer-host` | Producer | IP of the machine that produced the message | `192.168.0.220` |
| `host` | Producer | Producer's hostname or IP | `192.168.0.220` |
| `instance` | Producer | Monitored instance (scrape target) | `192.168.0.220` |
| `source-node` | Producer | K8s pod IP of the source (internal) | `10.42.0.11` |
| `source-endpoint` | Producer | Prometheus-style scrape endpoint URL | `http://10.42.0.11:9000/` |
| `public-ip` | Network | Public IP of the producer | `192.168.0.220` |
| `private-ip` | Network | Private/internal IP | `192.168.0.220` |
| `cloud` | Infra | Cloud provider | `aws`, `gcp`, or `${provider}` if unresolved |
| `region` | Infra | Cloud region | `eu-west-1` or `${zone-id}` if unresolved |
| `zone` | Infra | Availability zone | `eu-west-1a` or `${zone-id}` if unresolved |
| `metric` | Metric | Prometheus metric name being reported | `request_processing_seconds_sum` |
| `priority` | STOMP | JMS priority (0–9, default 4 = normal) | `4` |
| `expires` | STOMP | Message TTL (`0` = never expires) | `0` |
| `content-type` | STOMP | Body content type | `application/json` |
| `correlation-id` | STOMP | Request/response correlation | `req-12345` |
| `reply-to` | STOMP | Reply destination | `/topic/responses` |

### Java-Style Message Normalization

Some legacy producers send messages in Java `toString()` format instead of JSON:

```
{action=CREATE, resource=dataset, params={name=test, owner=george}}
```

The client normalizes these automatically by converting `=` to `:`, quoting keys, and replacing single quotes — mirroring the Go-side `normalizeEMSMessage()` function in `ems_subscriber.go`.

## Installation

### Prerequisites

- Python 3.8+
- Access to the EMS broker (network connectivity to STOMP port)

### Setup

```bash
# Clone or copy to your tools directory
mkdir -p /opt/iccs/tools/EMSTopicConsumer
cd /opt/iccs/tools/EMSTopicConsumer

# Create virtual environment
sudo apt install python3-venv python3.12-venv -y   # if not installed
python3 -m venv venv
source venv/bin/activate

# Install dependency
pip install stomp.py

# Make executable
chmod +x ems_client.py
```

### Verify

```bash
source venv/bin/activate
python3 ems_client.py --version
# ems_client 2.0.0
```

## Usage

Always activate the virtual environment first:

```bash
cd /opt/iccs/tools/EMSTopicConsumer
source venv/bin/activate
```

### Listen Mode (Consumer)

```bash
# Listen to all topics (wildcard /topic/>)
python3 ems_client.py --host localhost --port 61610

# Show ALL headers grouped by category (Identity, Routing, Producer, etc.)
python3 ems_client.py --host localhost --port 61610 --headers

# Minimal display — destination and body only
python3 ems_client.py --host localhost --port 61610 --minimal

# Filter to SENSOR topics only (client-side substring match)
python3 ems_client.py --host localhost --port 61610 --filter-topic SENSOR

# Filter to a specific topic (broker-side)
python3 ems_client.py --host localhost --port 61610 --topic /topic/cpu_util_instance

# Log all messages to JSONL file while displaying
python3 ems_client.py --host localhost --port 61610 --log messages.jsonl

# Verbose mode — show server info, tracebacks on errors
python3 ems_client.py --host localhost --port 61610 -v

# Non-durable subscription (no client-id persistence)
python3 ems_client.py --host localhost --port 61610 --no-durable

# Pipe-friendly (no colors, no interactive commands)
python3 ems_client.py --host localhost --port 61610 --no-color 2>/dev/null
```

### Interactive Commands (while listening)

| Key | Action |
|-----|--------|
| `s` + Enter | Send a message interactively (prompted for destination, action, resource, params) |
| `c` + Enter | Show count of messages received |
| `h` + Enter | Show help |
| `q` + Enter | Quit gracefully |
| `Ctrl+C` | Quit gracefully |

### Send Mode (Producer)

```bash
# Send a simple command
python3 ems_client.py --host localhost --port 61610 --send \
    --topic /topic/optimusdb.commands \
    --action CREATE \
    --resource dataset \
    --params '{"name":"energy_metrics","owner":"george"}'

# Send with correlation ID (for request/response patterns)
python3 ems_client.py --host localhost --port 61610 --send \
    --topic /topic/optimusdb.commands \
    --action QUERY \
    --resource metadata \
    --params '{"table":"renewable_sources"}' \
    --correlation-id "req-$(date +%s)" \
    --reply-to /topic/optimusdb.responses

# Delete action
python3 ems_client.py --host localhost --port 61610 --send \
    --topic /topic/optimusdb.commands \
    --action DELETE \
    --resource dataset \
    --params '{"name":"old_dataset"}'
```

## Configuration

All settings can be provided via command-line arguments or environment variables. CLI arguments take precedence.

| CLI Argument | Env Variable | Default | Description |
|-------------|-------------|---------|-------------|
| `--host` | `EMS_HOST` | `ems-broker.default.svc.cluster.local` | Broker hostname |
| `--port` | `EMS_PORT` / `EMS_STOMP_PORT` | `61610` | STOMP port |
| `--user` | `MQ_USER` | `aaa` | Broker username |
| `--password` | `MQ_PASS` | `111` | Broker password |
| `--client-id` | `MQ_CLIENT_ID` | Auto-generated | Client ID for durable subs |
| `--topic` | `EMS_TOPIC` | `/topic/>` | Topic to subscribe to |
| `--sub-name` | `EMS_SUB_NAME` | `ems-client-sub` | Durable subscription name |

### Example .env file

```bash
export EMS_HOST=localhost
export EMS_PORT=61610
export MQ_USER=aaa
export MQ_PASS=111
export MQ_CLIENT_ID=ems-debug-client
export EMS_TOPIC=/topic/>
```

Source it before running:

```bash
source .env
python3 ems_client.py
```

## Logging

### JSONL Format

When `--log FILE` is specified, every message is appended as a single JSON line:

```json
{
  "received_at": "2026-02-18T20:53:10.933000+00:00",
  "headers": {
    "destination": "/topic/response_time_SENSOR",
    "node-id": "da957267-058e-4e18-a4d5-3248db169c98",
    "timestamp": "1771447990930",
    "producer-host": "192.168.0.220",
    "instance": "192.168.0.220",
    "source-node": "10.42.0.11",
    "source-endpoint": "http://10.42.0.11:9000/",
    "metric": "request_processing_seconds_sum",
    "message-id": "ID:swch-monitoring-testbed-1-37457-1770380707376-4:271384:1:1:1",
    "priority": "4",
    "expires": "0"
  },
  "body": {
    "action": "",
    "resource": "",
    "params": {},
    "raw": "",
    "parsed": false
  }
}
```

### Analyzing Logs

```bash
# Count messages per topic
cat messages.jsonl | jq -r '.headers.destination' | sort | uniq -c | sort -rn

# Find all SENSOR messages
cat messages.jsonl | jq 'select(.headers.destination | contains("SENSOR"))'

# Extract unique producer hosts
cat messages.jsonl | jq -r '.headers["producer-host"]' | sort -u

# Get all unique metrics
cat messages.jsonl | jq -r '.headers.metric // empty' | sort -u

# Messages per minute
cat messages.jsonl | jq -r '.received_at[:16]' | sort | uniq -c

# Filter by action
cat messages.jsonl | jq 'select(.body.action == "CREATE")'
```

## Network Access

### From Inside the K8s Cluster

If running on a node in the K3s cluster:

```bash
python3 ems_client.py --host ems-broker.messaging.svc.cluster.local --port 61610
```

### From the K8s Host (epm-server)

If the broker has a NodePort or LoadBalancer service:

```bash
python3 ems_client.py --host localhost --port 61610
```

### From a Remote Machine (via SSH tunnel)

```bash
# Terminal 1: Port-forward via SSH
ssh -L 61610:localhost:61610 ubuntu@epm-server

# Terminal 2: Connect locally
python3 ems_client.py --host localhost --port 61610
```

Or using `kubectl port-forward`:

```bash
# Terminal 1
ssh ubuntu@epm-server "kubectl port-forward -n messaging svc/ems-broker 61610:61610 --address 0.0.0.0"

# Terminal 2
python3 ems_client.py --host localhost --port 61610
```

## Integration with OptimusDB

### How OptimusDB Consumes EMS

The OptimusDB Go agent connects to the same broker via `app/ems_subscriber.go`:

```go
// StartEMSSubscriber creates a STOMP connection and subscribes to topics.
// Messages are routed through:
//   frame.Body → handleEMSMessage() → ProcessEMS(action, resource, params)
//
// Environment variables (same as this client):
//   EMS_SERVICE_NAME  — broker host (default: ems-broker.default.svc.cluster.local)
//   EMS_STOMP_PORT    — STOMP port (default: 61610)
//   MQ_USER / MQ_PASS — credentials (default: aaa / 111)
//   EMS_TOPIC         — subscription topic (default: /topic/>)
```

### Message Flow

```
Producer → EMS Broker → /topic/...
                           │
                           ├─→ OptimusDB Node (Go)
                           │     handleEMSMessage()
                           │       → parse JSON body
                           │       → InsertEMSEvent() to optimuslog.db
                           │       → ProcessEMS(action, resource, params)
                           │
                           └─→ EMS Client (Python — this tool)
                                 parse_body()
                                   → display + optional JSONL log
```

### Sending Commands to OptimusDB

You can send messages that OptimusDB's `ProcessEMS()` will handle:

```bash
# Trigger a dataset creation
python3 ems_client.py --send \
    --topic /topic/optimusdb.commands \
    --action CREATE --resource dataset \
    --params '{"name":"test_ds"}'
```

The OptimusDB agent will receive this and process it in `app/service.go → ProcessEMS()`.

### Querying EMS Events Stored in OptimusDB

OptimusDB stores all received EMS events in `optimuslog.db`. Query them via the API:

```bash
# All EMS events (last 100)
curl "http://HOST:PORT/swarmkb/ems/events?limit=100"

# Filter by action
curl "http://HOST:PORT/swarmkb/ems/sql?q=SELECT * FROM ems_events WHERE action='CREATE' ORDER BY received_at DESC LIMIT 20"

# EMS log entries
curl "http://HOST:PORT/swarmkb/ems/logs?level=INFO&since_min=60&limit=50"
```

## File Structure

```
EMSTopicConsumer/
├── ems_client.py          # Main client script
├── README.md              # This documentation
├── .env.example           # Example environment configuration
├── venv/                  # Python virtual environment (not committed)
│   ├── bin/
│   ├── lib/
│   └── ...
└── logs/                  # JSONL log output directory (gitignored)
    └── messages.jsonl
```

## Troubleshooting

### Connection Refused

```
stomp.exception.ConnectFailedException: Connection refused
```

- Verify the broker is running: `kubectl get pods -A | grep ems`
- Check the service: `kubectl get svc -A | grep ems`
- Verify the port: `nc -zv localhost 61610`

### Immediate Disconnect

```
✓ Connected to EMS broker
✗ Disconnected from EMS broker
```

- Check broker logs: `kubectl logs -n messaging <broker-pod>`
- Try without durable subscription: `--no-durable`
- Verify credentials: `--user aaa --password 111`

### No Messages Received

- Verify topic wildcard: `--topic /topic/>` subscribes to ALL topics
- Check if messages are being produced: look at broker admin console
- Try a more specific topic: `--topic /topic/response_time_SENSOR`
- Check client-side filter isn't too restrictive: remove `--filter-topic`

### Python venv Issues

```
python3 -m venv: ensurepip is not available
```

Install the venv package for your Python version:

```bash
sudo apt install python3.12-venv -y
```

### Unresolved Template Variables

If you see headers like `cloud: ${provider}` or `region: ${zone-id}`, these are unresolved template variables in the SwarmChestrate monitoring configuration. They need to be set in the producer's deployment config, not in this client.

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| [stomp.py](https://pypi.org/project/stomp.py/) | ≥8.0 | STOMP protocol client |
| Python | ≥3.8 | Runtime |

## License

MIT License — Copyright (c) 2025-2026 OptimusDB / ICCS
