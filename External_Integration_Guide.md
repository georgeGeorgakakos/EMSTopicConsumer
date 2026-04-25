# EMS Topic Consumer — External Integration Guide

A standalone Python STOMP client for consuming and producing messages on the EMS (Enterprise Monitoring System) broker. This repository serves as a **reference implementation and ready-to-use tool** for any external application that needs to integrate with the EMS event bus.

> **Part of the [SwarmChestrate](https://swarmchestrate.2siti.si/) project** — Horizon Europe Grant #101135012

## Table of Contents

- [Why This Exists](#why-this-exists)
- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Network Access Patterns](#network-access-patterns)
- [Configuration Reference](#configuration-reference)
- [Usage — Listen Mode (Consumer)](#usage--listen-mode-consumer)
- [Usage — Send Mode (Producer)](#usage--send-mode-producer)
- [Interactive Commands](#interactive-commands)
- [Message Formats](#message-formats)
- [Header Reference](#header-reference)
- [Integrating Your Own Application](#integrating-your-own-application)
- [Logging & Analysis](#logging--analysis)
- [Integration with OptimusDB](#integration-with-optimusdb)
- [Troubleshooting](#troubleshooting)
- [File Structure](#file-structure)
- [License](#license)

---

## Why This Exists

The EMS broker is the central event bus for the SwarmChestrate monitoring platform. It runs on ActiveMQ and exposes a **STOMP** (Simple Text-Oriented Messaging Protocol) interface on port `61610`. All monitoring telemetry, commands, and events flow through STOMP topics.

Any external application — whether written in Python, Go, Java, Node.js, or any language with a STOMP library — can connect to the EMS broker and consume or produce messages. This repository provides:

1. **A ready-to-run Python client** (`EMSClient.py`) for monitoring, debugging, and sending messages.
2. **A reference implementation** showing the exact connection parameters, authentication, topic structure, message formats, and header conventions needed to build your own integration in any language.
3. **Documentation** of the SSH tunneling pattern required when the EMS broker runs on a separate VM.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                       EMS Broker                            │
│                     (ActiveMQ STOMP)                        │
│                      Port: 61610                            │
│                                                             │
│  /topic/response_time_SENSOR   ← monitoring telemetry       │
│  /topic/cpu_util_instance      ← CPU metrics                │
│  /topic/optimusdb.commands     ← command channel            │
│  /topic/optimusdb.events       ← event notifications        │
│  /topic/>                      ← wildcard (all topics)      │
└────────┬──────────────┬─────────────────┬───────────────────┘
         │              │                 │
         ▼              ▼                 ▼
   ┌──────────┐  ┌──────────┐     ┌──────────────────┐
   │OptimusDB │  │OptimusDB │     │ Your Application │
   │ Node 1   │  │ Node 2   │     │                  │
   │ (Go)     │  │ (Go)     │     │ Any language     │
   └──────────┘  └──────────┘     │ with STOMP lib   │
                                  └──────────────────┘
```

When the EMS broker runs on a different VM from your application, connectivity is established via an **SSH tunnel** — no firewall changes are required. Only SSH port 22 must be open between the two machines.

```
┌─────────────────────────┐      SSH Tunnel (port 22)      ┌─────────────────────────┐
│   Your Application VM   │  ============================  │       EMS VM            │
│                         │                                │  (193.225.251.34)       │
│  ┌───────────────────┐  │                                │  ┌───────────────────┐  │
│  │ autossh service   │──│── 0.0.0.0:61610 ───────────►   │  │ ActiveMQ (Java)   │  │
│  │ (ems-tunnel)      │  │    localhost:61610              │  │ *:61610 (STOMP)   │  │
│  └───────────────────┘  │                                │  └───────────────────┘  │
│         ▲               │                                └─────────────────────────┘
│         │               │
│  ┌───────────────────┐  │
│  │ Your App / Client │  │
│  │ connects to       │  │
│  │ localhost:61610   │  │
│  └───────────────────┘  │
└─────────────────────────┘
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Python** | 3.8 or higher |
| **Network** | Connectivity to the EMS broker's STOMP port (61610) — either directly or via SSH tunnel |
| **Software** | `autossh` (only if using SSH tunneling to a remote EMS VM) |
| **SSH Keys** | RSA or ED25519 keypair if tunneling (public key must be authorized on the EMS VM) |
| **STOMP Credentials** | Default: username `aaa`, password `111` |

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/georgeGeorgakakos/EMSTopicConsumer.git
cd EMSTopicConsumer

# 2. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install the dependency
pip install stomp.py

# 4. Run the client (assuming EMS is reachable on localhost:61610)
python3 EMSClient.py --host localhost --port 61610
```

If the EMS broker is on a remote VM, see [Network Access Patterns](#network-access-patterns) first.

---

## Network Access Patterns

### Pattern 1 — Same Machine / Same Cluster

If your application runs on the same machine or within the same K3s cluster as the EMS broker:

```bash
python3 EMSClient.py --host localhost --port 61610

# Or using the in-cluster DNS name:
python3 EMSClient.py --host ems-broker.messaging.svc.cluster.local --port 61610
```

### Pattern 2 — Remote Machine via SSH Tunnel (Recommended for Cross-VM)

When the EMS broker is on a separate VM that only exposes SSH (port 22):

**Step 1 — Set up the SSH tunnel on your machine:**

```bash
# Install autossh
sudo apt install autossh -y

# Generate SSH keys if needed
ls ~/.ssh/id_rsa.pub 2>/dev/null || ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa

# Copy your public key to the EMS VM
ssh-copy-id ubuntu@<EMS_VM_IP>

# Test SSH connectivity
ssh ubuntu@<EMS_VM_IP> 'hostname && echo SSH OK'
```

**Step 2 — Create a persistent tunnel as a systemd service:**

```bash
sudo tee /etc/systemd/system/ems-tunnel.service << 'EOF'
[Unit]
Description=SSH Tunnel to EMS Server (STOMP 61610)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
ExecStart=/usr/bin/autossh -M 0 -N \
  -o "ServerAliveInterval=30" \
  -o "ServerAliveCountMax=3" \
  -o "ExitOnForwardFailure=yes" \
  -L 0.0.0.0:61610:localhost:61610 ubuntu@<EMS_VM_IP>
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ems-tunnel
sudo systemctl start ems-tunnel
```

> **Important:** The tunnel binds on `0.0.0.0:61610` (not `127.0.0.1`) so that Kubernetes pods or Docker containers on the same host can reach it via the host's IP.

**Step 3 — Verify the tunnel:**

```bash
sudo systemctl status ems-tunnel
ss -tlnp | grep 61610        # Should show 0.0.0.0:61610
nc -zv 127.0.0.1 61610       # Should report "succeeded"
```

**Step 4 — Connect:**

```bash
python3 EMSClient.py --host localhost --port 61610
```

### Pattern 3 — Ad-Hoc SSH Tunnel (Development / Testing)

For quick testing without a persistent service:

```bash
# Terminal 1: Open the tunnel
ssh -L 61610:localhost:61610 ubuntu@<EMS_VM_IP>

# Terminal 2: Run the client
python3 EMSClient.py --host localhost --port 61610
```

### Pattern 4 — Kubernetes Pod Access (via Host Tunnel)

If your application runs as a K8s pod and the SSH tunnel is on the host, configure the pod's environment to use `status.hostIP` (the node's IP) as the broker address:

```yaml
env:
  - name: EMS_HOST
    valueFrom:
      fieldRef:
        fieldPath: status.hostIP
  - name: EMS_PORT
    value: "61610"
```

> Inside a K8s pod, `localhost` refers to the pod's own network namespace — **not** the host machine where the SSH tunnel runs. Always use `status.hostIP`.

---

## Configuration Reference

All settings can be provided via CLI arguments or environment variables. CLI arguments take precedence.

| CLI Argument | Environment Variable | Default | Description |
|---|---|---|---|
| `--host` | `EMS_HOST` | `ems-broker.default.svc.cluster.local` | STOMP broker hostname or IP |
| `--port` | `EMS_PORT` / `EMS_STOMP_PORT` | `61610` | STOMP protocol port |
| `--user` | `MQ_USER` | `aaa` | Broker username |
| `--password` | `MQ_PASS` | `111` | Broker password |
| `--client-id` | `MQ_CLIENT_ID` | Auto-generated | Unique client ID for durable subscriptions |
| `--topic` | `EMS_TOPIC` | `/topic/>` | STOMP topic to subscribe to (`>` is ActiveMQ wildcard for all) |
| `--sub-name` | `EMS_SUB_NAME` | `ems-client-sub` | Durable subscription name |

### Example `.env` File

```bash
export EMS_HOST=localhost
export EMS_PORT=61610
export MQ_USER=aaa
export MQ_PASS=111
export MQ_CLIENT_ID=my-external-app
export EMS_TOPIC=/topic/>
```

Source it before running:

```bash
source .env
python3 EMSClient.py
```

---

## Usage — Listen Mode (Consumer)

Always activate the virtual environment first:

```bash
cd EMSTopicConsumer
source venv/bin/activate
```

```bash
# Listen to all topics (wildcard)
python3 EMSClient.py --host localhost --port 61610

# Show ALL headers grouped by category (Identity, Routing, Producer, etc.)
python3 EMSClient.py --host localhost --port 61610 --headers

# Minimal display — destination and body only
python3 EMSClient.py --host localhost --port 61610 --minimal

# Filter to SENSOR topics only (client-side substring match)
python3 EMSClient.py --host localhost --port 61610 --filter-topic SENSOR

# Filter to a specific topic (broker-side)
python3 EMSClient.py --host localhost --port 61610 --topic /topic/cpu_util_instance

# Log all messages to JSONL file while displaying
python3 EMSClient.py --host localhost --port 61610 --log messages.jsonl

# Verbose mode — show server info, tracebacks on errors
python3 EMSClient.py --host localhost --port 61610 -v

# Non-durable subscription (no client-id persistence)
python3 EMSClient.py --host localhost --port 61610 --no-durable

# Pipe-friendly (no colors, no interactive commands)
python3 EMSClient.py --host localhost --port 61610 --no-color 2>/dev/null
```

---

## Usage — Send Mode (Producer)

```bash
# Send a simple command
python3 EMSClient.py --host localhost --port 61610 --send \
    --topic /topic/optimusdb.commands \
    --action CREATE \
    --resource dataset \
    --params '{"name":"energy_metrics","owner":"george"}'

# Send with correlation ID (for request/response patterns)
python3 EMSClient.py --host localhost --port 61610 --send \
    --topic /topic/optimusdb.commands \
    --action QUERY \
    --resource metadata \
    --params '{"table":"renewable_sources"}' \
    --correlation-id "req-$(date +%s)" \
    --reply-to /topic/optimusdb.responses

# Delete action
python3 EMSClient.py --host localhost --port 61610 --send \
    --topic /topic/optimusdb.commands \
    --action DELETE \
    --resource dataset \
    --params '{"name":"old_dataset"}'
```

---

## Interactive Commands

While in listen mode, the following keyboard commands are available:

| Key | Action |
|---|---|
| `s` + Enter | Send a message interactively (prompted for destination, action, resource, params) |
| `c` + Enter | Show count of messages received |
| `h` + Enter | Show help |
| `q` + Enter | Quit gracefully |
| `Ctrl+C` | Quit gracefully |

---

## Message Formats

The EMS ecosystem uses two distinct message formats. Your application must handle both depending on the topics you subscribe to.

### Format 1 — Body-Based JSON (Commands / Events)

Standard OptimusDB messages carry structured data in the STOMP body:

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

The three fields (`action`, `resource`, `params`) form the standard message contract.

### Format 2 — Header-Based (Monitoring / SENSOR Telemetry)

Monitoring messages from the SwarmChestrate platform carry all data in STOMP headers with an **empty body**:

```
Headers:
  destination:       /topic/response_time_SENSOR
  node-id:           da957267-058e-4e18-a4d5-3248db169c98
  timestamp:         1771447990930
  producer-host:     192.168.0.220
  instance:          192.168.0.220
  source-node:       10.42.0.11
  metric:            request_processing_seconds_sum
  cloud:             aws
  region:            eu-west-1
  priority:          4
  expires:           0

Body: (empty)
```

### Java-Style Normalization

Some legacy producers emit messages in Java `toString()` format instead of JSON:

```
{action=CREATE, resource=dataset, params={name=test, owner=george}}
```

The client automatically normalizes these by converting `=` to `:`, quoting keys, and replacing single quotes. If you are building your own integration, implement the same normalization — see the `normalize_ems_message()` function in `EMSClient.py`.

---

## Header Reference

| Header | Group | Description | Example |
|---|---|---|---|
| `message-id` | Identity | Unique broker-assigned ID | `ID:swch-monitoring-testbed-1-37457-...` |
| `node-id` | Identity | UUID of the producing node/agent | `da957267-058e-4e18-a4d5-3248db169c98` |
| `timestamp` | Identity | Unix epoch in milliseconds | `1771447990930` |
| `destination` | Routing | Topic the message was delivered on | `/topic/response_time_SENSOR` |
| `destination-topic` | Routing | Short topic name (no `/topic/` prefix) | `response_time_SENSOR` |
| `effective-destination` | Routing | Resolved destination after broker rules | `cpu_util_instance_SENSOR` |
| `original-destination` | Routing | Where the producer originally sent it | `cpu_util_instance_SENSOR` |
| `producer-host` | Producer | IP of the producing machine | `192.168.0.220` |
| `instance` | Producer | Monitored instance (scrape target) | `192.168.0.220` |
| `source-node` | Producer | K8s pod IP of the source (internal) | `10.42.0.11` |
| `source-endpoint` | Producer | Prometheus-style scrape endpoint URL | `http://10.42.0.11:9000/` |
| `metric` | Metric | Prometheus metric name being reported | `request_processing_seconds_sum` |
| `cloud` | Infra | Cloud provider | `aws`, `gcp`, or `${provider}` |
| `region` | Infra | Cloud region | `eu-west-1` or `${zone-id}` |
| `zone` | Infra | Availability zone | `eu-west-1a` or `${zone-id}` |
| `priority` | STOMP | JMS priority (0–9, default 4) | `4` |
| `expires` | STOMP | Message TTL (`0` = never) | `0` |
| `content-type` | STOMP | Body content type | `application/json` |
| `correlation-id` | STOMP | Request/response correlation | `req-12345` |
| `reply-to` | STOMP | Reply destination | `/topic/responses` |

> Headers with values like `${provider}` or `${zone-id}` are unresolved template variables in the SwarmChestrate monitoring configuration and need to be set on the producer side.

---

## Integrating Your Own Application

This section explains how to build your own EMS consumer in **any language**. The EMSClient.py in this repo is the Python reference implementation.

### Step 1 — Choose a STOMP Client Library

| Language | Library | Install |
|---|---|---|
| **Python** | [stomp.py](https://pypi.org/project/stomp.py/) | `pip install stomp.py` |
| **Go** | [go-stomp](https://github.com/go-stomp/stomp) | `go get github.com/go-stomp/stomp/v3` |
| **Java** | Spring STOMP / ActiveMQ Client | Maven/Gradle dependency |
| **Node.js** | [stompit](https://www.npmjs.com/package/stompit) | `npm install stompit` |
| **C#/.NET** | [Apache.NMS.STOMP](https://activemq.apache.org/components/nms/) | NuGet package |
| **Rust** | [stomp-rs](https://crates.io/crates/stomp) | `cargo add stomp` |

### Step 2 — Establish Connectivity

Ensure your application can reach the broker on port `61610`. If the broker is on a different VM, set up the SSH tunnel as described in [Network Access Patterns](#network-access-patterns).

### Step 3 — Connect and Subscribe

Here is the minimal integration pattern in pseudocode:

```
1. Connect to STOMP broker at <host>:61610
   - username: "aaa"
   - password: "111"
   - client-id: "<unique-id-per-instance>"    # for durable subscriptions
   - heartbeat: send=10s, recv=10s

2. Subscribe to topic
   - destination: "/topic/>"                   # wildcard — all topics
   - OR a specific topic like "/topic/cpu_util_instance"
   - ack: "auto"
   - For durable subscriptions, set header:
     activemq.subscriptionName: "<subscription-name>"

3. On each message:
   a. Read STOMP headers (dict/map)
   b. Read STOMP body (may be empty for SENSOR messages)
   c. If body is non-empty:
      - Try JSON.parse(body)
      - If that fails, try normalize_ems_message(body)  # Java-style
      - Extract: action, resource, params
   d. If body is empty:
      - All data is in headers (monitoring/telemetry)
      - Key fields: destination, node-id, metric, instance, timestamp

4. Implement reconnection with exponential backoff:
   - Initial delay: 5 seconds
   - Max delay: 5 minutes
   - Re-subscribe on reconnect
```

### Step 4 — Example Implementations

#### Python (Minimal Consumer)

```python
import stomp
import json

class MyListener(stomp.ConnectionListener):
    def on_message(self, frame):
        headers = dict(frame.headers)
        topic = headers.get("destination", "")
        body = frame.body

        if body and body.strip():
            data = json.loads(body)
            print(f"[{topic}] action={data.get('action')} resource={data.get('resource')}")
        else:
            # Header-based message (SENSOR/monitoring)
            metric = headers.get("metric", "N/A")
            node_id = headers.get("node-id", "N/A")
            print(f"[{topic}] metric={metric} node={node_id}")

    def on_error(self, frame):
        print(f"ERROR: {frame.body}")

conn = stomp.Connection([("localhost", 61610)], heartbeats=(10000, 10000))
conn.set_listener("my_listener", MyListener())
conn.connect(username="aaa", passcode="111", wait=True,
             headers={"client-id": "my-app-001"})
conn.subscribe(destination="/topic/>", id="sub-1", ack="auto",
               headers={"activemq.subscriptionName": "my-app-sub"})

# Keep alive
import time
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    conn.disconnect()
```

#### Go (Minimal Consumer)

```go
package main

import (
    "fmt"
    "log"
    "github.com/go-stomp/stomp/v3"
)

func main() {
    conn, err := stomp.Dial("tcp", "localhost:61610",
        stomp.ConnOpt.Login("aaa", "111"),
        stomp.ConnOpt.HeartBeat(10*time.Second, 10*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Disconnect()

    sub, err := conn.Subscribe("/topic/>", stomp.AckAuto)
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Unsubscribe()

    for msg := range sub.C {
        dest := msg.Header.Get("destination")
        body := string(msg.Body)
        fmt.Printf("[%s] %s\n", dest, body)
    }
}
```

#### Node.js (Minimal Consumer)

```javascript
const stompit = require("stompit");

const connectOptions = {
  host: "localhost",
  port: 61610,
  connectHeaders: {
    login: "aaa",
    passcode: "111",
    "client-id": "nodejs-consumer-001",
    "heart-beat": "10000,10000",
  },
};

stompit.connect(connectOptions, (error, client) => {
  if (error) { console.error("Connect error:", error); return; }

  const subscribeHeaders = {
    destination: "/topic/>",
    ack: "auto",
    "activemq.subscriptionName": "nodejs-sub",
  };

  client.subscribe(subscribeHeaders, (error, message) => {
    if (error) { console.error("Subscribe error:", error); return; }

    message.readString("utf-8", (error, body) => {
      const dest = message.headers.destination;
      if (body && body.trim()) {
        const data = JSON.parse(body);
        console.log(`[${dest}] action=${data.action} resource=${data.resource}`);
      } else {
        const metric = message.headers.metric || "N/A";
        console.log(`[${dest}] metric=${metric}`);
      }
    });
  });
});
```

### Step 5 — Production Considerations

When moving from a debug/testing scenario to production, keep these in mind:

1. **Unique Client IDs** — Every consumer instance must have a unique `client-id` to avoid subscription conflicts. Use the pod name, hostname, or a UUID.
2. **Durable Subscriptions** — Set `activemq.subscriptionName` in the subscribe headers so the broker retains messages while your consumer is temporarily offline.
3. **Reconnection Logic** — Implement exponential backoff (5s initial, 5min max). The `EMSClient.py` reference uses `stomp.py`'s built-in reconnection with `reconnect_attempts_max=-1` (infinite).
4. **Health Checks** — Periodically send a ping to `/queue/optimusdb-health` or implement heartbeat monitoring to detect stale connections.
5. **Credential Management** — For production K8s deployments, store STOMP credentials in Kubernetes Secrets rather than environment variables or hardcoded values:
   ```bash
   kubectl create secret generic ems-credentials -n <namespace> \
     --from-literal=MQ_USER=aaa --from-literal=MQ_PASS=111
   ```
6. **Topic Filtering** — Subscribe to specific topics instead of the wildcard `/topic/>` to reduce unnecessary message processing.
7. **Network Policies** — In multi-tenant K8s environments, add NetworkPolicies to restrict which pods can access port 61610 on the host.

---

## Logging & Analysis

When `--log FILE` is specified, every message is appended as a single JSON line (JSONL format):

```json
{
  "received_at": "2026-02-18T20:53:10.933000+00:00",
  "headers": {
    "destination": "/topic/response_time_SENSOR",
    "node-id": "da957267-...",
    "metric": "request_processing_seconds_sum"
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

### Useful `jq` Queries

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

---

## Integration with OptimusDB

### How OptimusDB Consumes EMS

The [OptimusDB](https://github.com/georgeGeorgakakos/optimusdb) distributed database cluster connects to the same broker via its Go-based `app/ems_subscriber.go` module. It uses the identical environment variables (`EMS_SERVICE_NAME`, `EMS_STOMP_PORT`, `MQ_USER`, `MQ_PASS`, `EMS_TOPIC`) and processes messages through a pipeline: parse body → normalize if needed → store in SQLite (`ems_events` table) → route to domain logic.

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
                           └─→ Your External Application
                                 (using this client or your own STOMP integration)
```

### Sending Commands to OptimusDB

You can send messages that OptimusDB's `ProcessEMS()` handler will receive:

```bash
python3 EMSClient.py --send \
    --topic /topic/optimusdb.commands \
    --action CREATE --resource dataset \
    --params '{"name":"test_ds"}'
```

### Querying EMS Events Stored in OptimusDB

OptimusDB persists all received EMS events. Query them via its REST API:

```bash
# All EMS events (last 100)
curl "http://<OPTIMUSDB_HOST>:<PORT>/swarmkb/ems/events?limit=100"

# Filter by action
curl "http://<OPTIMUSDB_HOST>:<PORT>/swarmkb/ems/sql?q=SELECT * FROM ems_events WHERE action='CREATE' ORDER BY received_at DESC LIMIT 20"

# EMS log entries
curl "http://<OPTIMUSDB_HOST>:<PORT>/swarmkb/ems/logs?level=INFO&since_min=60&limit=50"
```

---

## Troubleshooting

| Symptom | Cause | Solution |
|---|---|---|
| `Connection refused` on localhost:61610 | SSH tunnel not running or broker is down | Check `sudo systemctl status ems-tunnel` and verify port with `nc -zv 127.0.0.1 61610` |
| `dial tcp: lookup ems-broker...no such host` | Using default DNS name without SSH tunnel | Set `--host localhost` or the correct IP/hostname |
| Connects then immediately disconnects | Bad credentials or client-id conflict | Try `--no-durable` and verify credentials with `--user aaa --password 111` |
| Tunnel keeps restarting (exit 255) | SSH config typo (e.g., `ServerAliveCountsec`) | Fix to `ServerAliveCountMax` in the systemd unit |
| `Permission denied (publickey)` | Public key not authorized on EMS VM | Run `ssh-copy-id ubuntu@<EMS_VM_IP>` |
| Connected but no messages | Wrong topic or nothing being published | Use `--topic /topic/>` (wildcard) and verify EMS client DaemonSet is running on the EMS VM |
| Port 61610 already in use | Another tunnel or process bound to it | Check with `ss -tlnp | grep 61610` and kill the conflicting process |
| `python3 -m venv: ensurepip not available` | Missing venv package | `sudo apt install python3.12-venv -y` (adjust version as needed) |
| Headers show `${provider}` or `${zone-id}` | Unresolved template variables | These must be configured on the producer side, not in this client |

### Diagnostic Commands

```bash
# Check tunnel
sudo systemctl status ems-tunnel
sudo journalctl -u ems-tunnel -f

# Check port
ss -tlnp | grep 61610

# Test STOMP connectivity
nc -zv 127.0.0.1 61610

# Check EMS pods on the EMS VM
sudo kubectl get pods -o wide | grep ems
```

---

## File Structure

```
EMSTopicConsumer/
├── EMSClient.py           # Main client script (829 lines, single-file)
├── README.md              # This integration guide
├── .env.example           # Example environment configuration
├── venv/                  # Python virtual environment (not committed)
└── logs/                  # JSONL log output directory (gitignored)
    └── messages.jsonl
```

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| [stomp.py](https://pypi.org/project/stomp.py/) | ≥ 8.0 | STOMP protocol client for Python |
| Python | ≥ 3.8 | Runtime |

---

## License

MIT License — Copyright (c) 2025–2026 OptimusDB / ICCS

---

## Related Resources

- [OptimusDB Repository](https://github.com/georgeGeorgakakos/optimusdb) — The distributed data catalog that consumes EMS events natively
- [OptimusDB EMS Integration Guide (DOCX)](./docs/OptimusDB-EMS-Integration-Guide.docx) — Detailed cross-VM integration documentation with Kubernetes manifest examples
- [SwarmChestrate Project](https://swarmchestrate.2siti.si/) — EU Horizon Europe project (Grant #101135012)
- [STOMP Protocol Specification](https://stomp.github.io/stomp-specification-1.2.html) — The underlying messaging protocol
- [ActiveMQ STOMP Documentation](https://activemq.apache.org/stomp) — Broker-specific STOMP features and configuration
