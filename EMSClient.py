#!/usr/bin/env python3
"""
OptimusDB EMS Client — STOMP Topic Consumer & Producer
=======================================================
Connects to the EMS broker (ActiveMQ/TIBCO EMS via STOMP) and subscribes
to topics. Displays received messages in real-time with headers and parsed
fields. Can also send messages to trigger OptimusDB actions.

Supports two message formats:
  1. Body-based JSON  — {"action":"CREATE","resource":"dataset","params":{...}}
  2. Header-based     — Monitoring/telemetry where data lives in STOMP headers
                        (e.g., SwarmChestrate SENSOR topics with empty body)

Requires: pip install stomp.py

Copyright (c) 2025-2026 OptimusDB / ICCS
License: MIT
"""

__version__ = "2.0.0"

import argparse
import json
import os
import re
import select
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

try:
    import stomp
except ImportError:
    print("ERROR: stomp.py not installed.")
    print("  pip install stomp.py")
    print("  # or inside a venv:")
    print("  python3 -m venv venv && source venv/bin/activate && pip install stomp.py")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration Defaults (from OptimusDB ems_subscriber.go)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULTS = {
    "host":      os.getenv("EMS_HOST", "ems-broker.default.svc.cluster.local"),
    "port":      int(os.getenv("EMS_PORT", os.getenv("EMS_STOMP_PORT", "61610"))),
    "user":      os.getenv("MQ_USER", "aaa"),
    "password":  os.getenv("MQ_PASS", "111"),
    "client_id": os.getenv("MQ_CLIENT_ID", f"ems-client-{uuid.uuid4().hex[:8]}"),
    "topic":     os.getenv("EMS_TOPIC", "/topic/>"),
    "sub_name":  os.getenv("EMS_SUB_NAME", "ems-client-sub"),
}


# ─────────────────────────────────────────────────────────────────────────────
# ANSI Terminal Colors
# ─────────────────────────────────────────────────────────────────────────────

class C:
    """ANSI color codes for terminal output."""
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[31m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    BLUE    = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN    = "\033[36m"
    WHITE   = "\033[37m"
    BG_DARK = "\033[48;5;236m"

    @staticmethod
    def disable():
        """Remove all color codes (for piped output or --no-color)."""
        for attr in dir(C):
            if attr.isupper() and not attr.startswith("_"):
                setattr(C, attr, "")


# ─────────────────────────────────────────────────────────────────────────────
# Message Parsing
# ─────────────────────────────────────────────────────────────────────────────

def normalize_ems_message(raw: str) -> str:
    """
    Convert Java-style {key=value, ...} into valid JSON.
    Mirrors optimusdb/app/ems_subscriber.go normalizeEMSMessage().
    """
    out = []
    in_quotes = False
    for ch in raw:
        if ch == '"':
            in_quotes = not in_quotes
        if not in_quotes and ch == '=':
            out.append(':')
        else:
            out.append(ch)
    result = ''.join(out)

    result = result.replace("'", '"')
    result = re.sub(r'([,{]\s*)([A-Za-z0-9_]+)(\s*:)', r'\1"\2"\3', result)

    return result


def parse_body(body) -> dict:
    """
    Parse EMS message body. Tries JSON first, then normalized Java format.
    Handles both bytes (stomp.py <8) and str (stomp.py 8+).

    Returns:
        dict with keys: raw, parsed, action, resource, params
    """
    if isinstance(body, bytes):
        raw = body.decode("utf-8", errors="replace")
    elif body is None:
        raw = ""
    else:
        raw = str(body)

    result = {
        "raw": raw,
        "parsed": False,
        "action": "",
        "resource": "",
        "params": {},
    }

    if not raw.strip():
        return result

    # Try direct JSON
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            result.update(data)
            result["parsed"] = True
        return result
    except (json.JSONDecodeError, TypeError):
        pass

    # Try normalized (Java-style key=value)
    normalized = normalize_ems_message(raw)
    if normalized:
        try:
            data = json.loads(normalized)
            if isinstance(data, dict):
                result.update(data)
                result["parsed"] = True
                result["normalized_from"] = raw
            return result
        except (json.JSONDecodeError, TypeError):
            pass

    return result


def classify_headers(headers: dict) -> dict:
    """
    Classify STOMP headers into logical groups for display.

    Groups:
        identity    — message-id, node-id, timestamp
        routing     — destination, destination-topic, effective-destination, etc.
        producer    — producer-host, host, instance, source-node, source-endpoint
        network     — public-ip, private-ip
        infra       — cloud, region, zone
        metric      — metric (Prometheus metric name)
        stomp       — priority, expires, content-type (STOMP protocol)
        internal    — subscription, ack, content-length (hidden by default)
        custom      — anything not in the above categories
    """
    groups = {
        "identity": {},
        "routing": {},
        "producer": {},
        "network": {},
        "infra": {},
        "metric": {},
        "stomp": {},
        "internal": {},
        "custom": {},
    }

    classification = {
        # Identity
        "message-id": "identity", "node-id": "identity", "timestamp": "identity",
        # Routing
        "destination": "routing", "destination-topic": "routing",
        "effective-destination": "routing", "original-destination": "routing",
        # Producer
        "producer-host": "producer", "host": "producer", "instance": "producer",
        "source-node": "producer", "source-endpoint": "producer",
        # Network
        "public-ip": "network", "private-ip": "network",
        # Infrastructure
        "cloud": "infra", "region": "infra", "zone": "infra",
        # Metric
        "metric": "metric",
        # STOMP protocol
        "priority": "stomp", "expires": "stomp", "content-type": "stomp",
        "correlation-id": "stomp", "reply-to": "stomp",
        # Internal (hidden)
        "subscription": "internal", "ack": "internal", "content-length": "internal",
    }

    for key, val in headers.items():
        group = classification.get(key, "custom")
        groups[group][key] = val

    return groups


def format_timestamp(ts_str: str) -> str:
    """Convert millisecond epoch timestamp to human-readable format."""
    try:
        ts_ms = int(ts_str)
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
    except (ValueError, TypeError, OSError):
        return ts_str


# ─────────────────────────────────────────────────────────────────────────────
# Display
# ─────────────────────────────────────────────────────────────────────────────

_msg_counter = 0

# Group display config: (label, color)
GROUP_DISPLAY = {
    "identity":  ("Identity",       C.CYAN),
    "routing":   ("Routing",        C.YELLOW),
    "producer":  ("Producer",       C.GREEN),
    "network":   ("Network",        C.BLUE),
    "infra":     ("Infrastructure", C.MAGENTA),
    "metric":    ("Metric",         C.RED),
    "stomp":     ("STOMP",          C.DIM),
    "custom":    ("Custom",         C.WHITE),
}

# Action color map
ACTION_COLORS = {
    "CREATE": C.GREEN, "INSERT": C.GREEN, "ADD": C.GREEN, "POST": C.GREEN,
    "UPDATE": C.BLUE,  "MODIFY": C.BLUE,  "PUT": C.BLUE,  "PATCH": C.BLUE,
    "DELETE": C.RED,   "REMOVE": C.RED,   "DROP": C.RED,
    "QUERY": C.CYAN,   "GET": C.CYAN,     "SELECT": C.CYAN, "SEARCH": C.CYAN,
}


def display_message(headers: dict, msg: dict, mode: str = "key", verbose: bool = False):
    """
    Pretty-print a received EMS message.

    Args:
        headers:  Raw STOMP headers dict
        msg:      Parsed body dict from parse_body()
        mode:     "all" = show all grouped headers
                  "key" = show key headers only
                  "minimal" = destination + body only
        verbose:  Show extra debug info
    """
    global _msg_counter
    _msg_counter += 1

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    num = f"#{_msg_counter}"
    destination = headers.get("destination", "unknown")

    # Detect message type
    has_body = bool(msg.get("raw", "").strip())
    is_sensor = "SENSOR" in destination or "metric" in headers
    msg_type = "SENSOR" if is_sensor else ("JSON" if msg["parsed"] else ("RAW" if has_body else "EMPTY"))

    type_color = {
        "SENSOR": C.MAGENTA,
        "JSON": C.GREEN,
        "RAW": C.YELLOW,
        "EMPTY": C.DIM,
    }.get(msg_type, C.WHITE)

    # ── Header ──
    print(f"\n{C.BG_DARK}{C.BOLD}{C.CYAN} ┌─ EMS Message {num} ──────────────────────────────────────────{C.RESET}")
    print(f"{C.DIM}  │ Received:    {C.RESET}{ts}")
    print(f"{C.DIM}  │ Destination: {C.RESET}{C.YELLOW}{destination}{C.RESET}")
    print(f"{C.DIM}  │ Type:        {C.RESET}{type_color}{C.BOLD}{msg_type}{C.RESET}")

    # ── Broker timestamp (decoded) ──
    broker_ts = headers.get("timestamp")
    if broker_ts:
        print(f"{C.DIM}  │ Broker Time: {C.RESET}{format_timestamp(broker_ts)}")

    # ── Headers ──
    if mode == "all":
        # Grouped display
        groups = classify_headers(headers)
        for group_key, (label, color) in GROUP_DISPLAY.items():
            group_data = groups.get(group_key, {})
            if not group_data:
                continue
            print(f"{C.DIM}  │{C.RESET}")
            print(f"{C.DIM}  │ {color}{C.BOLD}── {label} ──{C.RESET}")
            for k, v in group_data.items():
                # Special formatting
                if k == "timestamp":
                    v_display = f"{v} → {format_timestamp(v)}"
                elif k == "source-endpoint":
                    v_display = f"{C.CYAN}{v}{C.RESET}"
                elif "${" in str(v):
                    v_display = f"{C.RED}{v} (unresolved){C.RESET}"
                else:
                    v_display = str(v)
                print(f"{C.DIM}  │   {k}: {C.RESET}{v_display}")

    elif mode == "key":
        # Show only important headers
        key_fields = [
            ("message-id", None),
            ("node-id", None),
            ("producer-host", None),
            ("source-node", None),
            ("source-endpoint", C.CYAN),
            ("metric", C.RED),
            ("instance", None),
            ("correlation-id", None),
            ("reply-to", None),
        ]
        shown = False
        for key, color in key_fields:
            val = headers.get(key)
            if val:
                if not shown:
                    print(f"{C.DIM}  │{C.RESET}")
                    shown = True
                if color:
                    print(f"{C.DIM}  │ {key}: {C.RESET}{color}{val}{C.RESET}")
                elif "${" in str(val):
                    print(f"{C.DIM}  │ {key}: {C.RESET}{C.RED}{val} (unresolved){C.RESET}")
                else:
                    print(f"{C.DIM}  │ {key}: {C.RESET}{val}")

        # Show any truly custom headers (not in standard set)
        standard = {
            "destination", "message-id", "node-id", "timestamp", "expires",
            "priority", "content-type", "content-length", "subscription", "ack",
            "producer-host", "host", "instance", "source-node", "source-endpoint",
            "public-ip", "private-ip", "cloud", "region", "zone", "metric",
            "destination-topic", "effective-destination", "original-destination",
            "correlation-id", "reply-to",
        }
        custom = {k: v for k, v in headers.items() if k not in standard}
        if custom:
            for k, v in custom.items():
                print(f"{C.DIM}  │ {k}: {C.RESET}{C.WHITE}{v}{C.RESET}")

    # ── Body ──
    print(f"{C.DIM}  │{C.RESET}")
    print(f"{C.DIM}  │ {C.BOLD}── Body ──{C.RESET}")

    if msg["parsed"]:
        action = msg.get("action", "")
        resource = msg.get("resource", "")
        params = msg.get("params", {})

        action_color = ACTION_COLORS.get(action.upper(), C.WHITE)

        if action:
            print(f"{C.DIM}  │ Action:      {C.RESET}{action_color}{C.BOLD}{action}{C.RESET}")
        if resource:
            print(f"{C.DIM}  │ Resource:    {C.RESET}{C.MAGENTA}{resource}{C.RESET}")

        if params:
            params_str = json.dumps(params, indent=4, ensure_ascii=False)
            for i, line in enumerate(params_str.split("\n")):
                print(f"{C.DIM}  │{C.RESET}   {C.WHITE}{line}{C.RESET}")

        # Show extra parsed fields (beyond action/resource/params)
        extra = {k: v for k, v in msg.items()
                 if k not in ("raw", "parsed", "action", "resource", "params", "normalized_from")}
        if extra:
            print(f"{C.DIM}  │ Extra fields:{C.RESET}")
            for k, v in extra.items():
                print(f"{C.DIM}  │   {k}: {C.RESET}{v}")

        if verbose and "normalized_from" in msg:
            print(f"{C.DIM}  │ (normalized from Java-style format){C.RESET}")

    elif has_body:
        # Unparsed body
        print(f"{C.DIM}  │ Status:      {C.RESET}{C.YELLOW}Unparsed{C.RESET}")
        raw = msg.get("raw", "")
        if len(raw) > 500:
            display_raw = raw[:500] + f"... ({len(raw)} bytes total)"
        else:
            display_raw = raw
        for line in display_raw.split("\n"):
            print(f"{C.DIM}  │{C.RESET}   {line}")
    else:
        # Empty body (typical for SENSOR messages)
        print(f"{C.DIM}  │ (empty — data is in headers){C.RESET}")

    # ── Footer ──
    print(f"{C.BG_DARK}{C.BOLD}{C.CYAN} └──────────────────────────────────────────────────────────────{C.RESET}")

    sys.stdout.flush()


# ─────────────────────────────────────────────────────────────────────────────
# STOMP Connection Listener
# ─────────────────────────────────────────────────────────────────────────────

class EMSListener(stomp.ConnectionListener):
    """
    STOMP connection listener.
    Parses and displays messages, optionally logs to JSONL file.
    """

    def __init__(self, header_mode: str = "key", verbose: bool = False,
                 log_file: str = None, topic_filter: str = None):
        self.header_mode = header_mode
        self.verbose = verbose
        self.log_file = log_file
        self.topic_filter = topic_filter
        self.log_fh = None
        self.connected = False

        if self.log_file:
            self.log_fh = open(self.log_file, "a", encoding="utf-8")
            print(f"{C.DIM}  Logging to: {self.log_file}{C.RESET}")

    def on_connected(self, frame):
        self.connected = True
        print(f"\n{C.GREEN}{C.BOLD}  ✓ Connected to EMS broker{C.RESET}")
        if self.verbose:
            print(f"{C.DIM}    Server:   {frame.headers.get('server', 'unknown')}{C.RESET}")
            print(f"{C.DIM}    Session:  {frame.headers.get('session', 'unknown')}{C.RESET}")
            print(f"{C.DIM}    Version:  {frame.headers.get('version', 'unknown')}{C.RESET}")
            print(f"{C.DIM}    Heart-bt: {frame.headers.get('heart-beat', 'unknown')}{C.RESET}")

    def on_disconnected(self):
        self.connected = False
        print(f"\n{C.RED}  ✗ Disconnected from EMS broker{C.RESET}")

    def on_error(self, frame):
        print(f"\n{C.RED}{C.BOLD}  ERROR from broker:{C.RESET} {frame.body}")
        if self.verbose and frame.headers:
            print(f"{C.DIM}    Headers: {frame.headers}{C.RESET}")

    def on_message(self, frame):
        try:
            headers = dict(frame.headers) if frame.headers else {}

            # Optional topic filter (client-side)
            if self.topic_filter:
                dest = headers.get("destination", "")
                if self.topic_filter not in dest:
                    return

            msg = parse_body(frame.body)

            # Display
            display_message(headers, msg, mode=self.header_mode, verbose=self.verbose)

            # Log to JSONL
            if self.log_fh:
                log_entry = {
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "headers": headers,
                    "body": {
                        "action": msg.get("action", ""),
                        "resource": msg.get("resource", ""),
                        "params": msg.get("params", {}),
                        "raw": msg.get("raw", ""),
                        "parsed": msg["parsed"],
                    },
                }
                self.log_fh.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
                self.log_fh.flush()

        except Exception as e:
            print(f"{C.RED}  ERROR processing message: {e}{C.RESET}")
            if self.verbose:
                import traceback
                traceback.print_exc()

    def on_heartbeat_timeout(self):
        print(f"\n{C.YELLOW}  ⚠ Heartbeat timeout — broker may be unreachable{C.RESET}")

    def on_receiver_loop_completed(self, frame):
        if self.verbose:
            print(f"{C.DIM}  Receiver loop completed{C.RESET}")

    def close(self):
        if self.log_fh:
            self.log_fh.close()
            self.log_fh = None


# ─────────────────────────────────────────────────────────────────────────────
# Connection Management
# ─────────────────────────────────────────────────────────────────────────────

def create_connection(args) -> tuple:
    """
    Create STOMP connection and subscribe.

    Returns:
        (stomp.Connection, EMSListener)
    """
    # Determine header display mode
    if args.headers:
        header_mode = "all"
    elif args.minimal:
        header_mode = "minimal"
    else:
        header_mode = "key"

    listener = EMSListener(
        header_mode=header_mode,
        verbose=args.verbose,
        log_file=getattr(args, "log", None),
        topic_filter=getattr(args, "filter_topic", None),
    )

    conn = stomp.Connection(
        [(args.host, args.port)],
        heartbeats=(10000, 10000),          # 10s send/recv (matches Go config)
        reconnect_sleep_initial=2.0,
        reconnect_sleep_increase=1.5,
        reconnect_sleep_max=30.0,
        reconnect_attempts_max=-1,          # infinite reconnect
    )
    conn.set_listener("ems", listener)

    # Connect
    connect_headers = {}
    if args.client_id and not args.no_durable:
        connect_headers["client-id"] = args.client_id

    print(f"{C.DIM}  Connecting to {args.host}:{args.port} ...{C.RESET}")
    conn.connect(
        username=args.user,
        passcode=args.password,
        wait=True,
        headers=connect_headers,
    )

    # Subscribe
    sub_headers = {}
    if args.client_id and not args.no_durable:
        sub_headers["activemq.subscriptionName"] = args.sub_name
        sub_headers["client-id"] = args.client_id
        durable_label = "durable"
    else:
        durable_label = "non-durable"

    print(f"{C.DIM}  Subscribing to: {C.BOLD}{args.topic}{C.RESET} ({durable_label})")
    conn.subscribe(
        destination=args.topic,
        id="ems-sub-1",
        ack="auto",
        headers=sub_headers,
    )

    return conn, listener


# ─────────────────────────────────────────────────────────────────────────────
# Send Functions
# ─────────────────────────────────────────────────────────────────────────────

def send_single(args):
    """Send a single EMS message and exit."""

    payload = {"action": args.action, "resource": args.resource}

    if args.params:
        try:
            payload["params"] = json.loads(args.params)
        except json.JSONDecodeError as e:
            print(f"{C.RED}ERROR: Invalid JSON for --params: {e}{C.RESET}")
            sys.exit(1)

    body = json.dumps(payload, ensure_ascii=False)

    conn = stomp.Connection([(args.host, args.port)])
    connect_headers = {}
    if args.client_id:
        connect_headers["client-id"] = args.client_id

    print(f"{C.DIM}  Connecting to {args.host}:{args.port} ...{C.RESET}")
    conn.connect(username=args.user, passcode=args.password, wait=True, headers=connect_headers)

    send_headers = {"content-type": "application/json"}
    if args.correlation_id:
        send_headers["correlation-id"] = args.correlation_id
    if args.reply_to:
        send_headers["reply-to"] = args.reply_to

    conn.send(destination=args.topic, body=body, headers=send_headers)

    print(f"{C.GREEN}  ✓ Sent to {args.topic}:{C.RESET}")
    print(f"    {json.dumps(payload, indent=2)}")
    if len(send_headers) > 1:
        print(f"{C.DIM}    Headers: {send_headers}{C.RESET}")

    conn.disconnect()
    print(f"{C.GREEN}  ✓ Disconnected{C.RESET}")


def interactive_send(conn, default_topic: str):
    """Prompt user to compose and send a message."""
    print(f"\n{C.CYAN}  ─── Send Message ───{C.RESET}")

    dest = input(f"    Destination [{C.BOLD}{default_topic}{C.RESET}]: ").strip()
    if not dest:
        dest = default_topic if default_topic != "/topic/>" else "/topic/optimusdb.commands"

    action = input(f"    Action   [{C.BOLD}CREATE{C.RESET}]: ").strip() or "CREATE"
    resource = input(f"    Resource [{C.BOLD}dataset{C.RESET}]: ").strip() or "dataset"
    params_raw = input(f"    Params JSON (or empty): ").strip()

    payload = {"action": action, "resource": resource}
    if params_raw:
        try:
            payload["params"] = json.loads(params_raw)
        except json.JSONDecodeError as e:
            print(f"{C.RED}    Invalid JSON: {e}{C.RESET}")
            return

    body = json.dumps(payload, ensure_ascii=False)

    try:
        conn.send(destination=dest, body=body, headers={"content-type": "application/json"})
        print(f"{C.GREEN}    ✓ Sent to {dest}{C.RESET}")
    except Exception as e:
        print(f"{C.RED}    ✗ Send failed: {e}{C.RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ems_client",
        description="OptimusDB EMS Client — STOMP Topic Consumer & Producer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
version: {__version__}

examples:
  # Listen to all topics with key headers
  %(prog)s --host localhost --port 61610

  # Show all headers (grouped and color-coded)
  %(prog)s --host localhost --port 61610 --headers

  # Minimal display (destination + body only)
  %(prog)s --host localhost --port 61610 --minimal

  # Filter to SENSOR topics only
  %(prog)s --host localhost --port 61610 --filter-topic SENSOR

  # Log all messages to JSONL file
  %(prog)s --host localhost --port 61610 --log messages.jsonl

  # Send a JSON message
  %(prog)s --host localhost --port 61610 --send \\
      --topic /topic/optimusdb.commands \\
      --action CREATE --resource dataset \\
      --params '{{"name":"test","owner":"george"}}'

  # Non-durable subscription (no client-id)
  %(prog)s --host localhost --port 61610 --no-durable

  # Pipe-friendly (no colors, no interactive)
  %(prog)s --host localhost --port 61610 --no-color 2>/dev/null

environment variables:
  EMS_HOST          Broker host     (default: ems-broker.default.svc.cluster.local)
  EMS_PORT          STOMP port      (default: 61610)
  MQ_USER           Username        (default: aaa)
  MQ_PASS           Password        (default: 111)
  MQ_CLIENT_ID      Client ID       (default: auto-generated)
  EMS_TOPIC         Topic           (default: /topic/>)
  EMS_SUB_NAME      Subscription    (default: ems-client-sub)
        """,
    )

    # ── Connection ──
    grp_conn = parser.add_argument_group("connection")
    grp_conn.add_argument("--host", default=DEFAULTS["host"],
                          help=f"STOMP broker host (default: {DEFAULTS['host']})")
    grp_conn.add_argument("--port", type=int, default=DEFAULTS["port"],
                          help=f"STOMP broker port (default: {DEFAULTS['port']})")
    grp_conn.add_argument("--user", default=DEFAULTS["user"],
                          help=f"Broker username (default: {DEFAULTS['user']})")
    grp_conn.add_argument("--password", default=DEFAULTS["password"],
                          help="Broker password")
    grp_conn.add_argument("--client-id", default=DEFAULTS["client_id"],
                          help="Client ID for durable subscription")
    grp_conn.add_argument("--topic", default=DEFAULTS["topic"],
                          help=f"Topic to subscribe (default: {DEFAULTS['topic']})")
    grp_conn.add_argument("--sub-name", default=DEFAULTS["sub_name"],
                          help=f"Durable subscription name (default: {DEFAULTS['sub_name']})")
    grp_conn.add_argument("--no-durable", action="store_true",
                          help="Disable durable subscription")

    # ── Send mode ──
    grp_send = parser.add_argument_group("send mode")
    grp_send.add_argument("--send", action="store_true",
                          help="Send a message and exit")
    grp_send.add_argument("--action", default="CREATE",
                          help="Message action (default: CREATE)")
    grp_send.add_argument("--resource", default="dataset",
                          help="Message resource (default: dataset)")
    grp_send.add_argument("--params", default=None,
                          help='Params as JSON string (e.g. \'{"name":"test"}\')')
    grp_send.add_argument("--correlation-id", default=None,
                          help="Correlation ID header")
    grp_send.add_argument("--reply-to", default=None,
                          help="Reply-to header")

    # ── Display ──
    grp_disp = parser.add_argument_group("display")
    grp_disp.add_argument("--headers", action="store_true",
                          help="Show ALL headers (grouped by category)")
    grp_disp.add_argument("--minimal", action="store_true",
                          help="Minimal display (destination + body only)")
    grp_disp.add_argument("--filter-topic", default=None, metavar="SUBSTRING",
                          help="Only show messages whose destination contains SUBSTRING")
    grp_disp.add_argument("--log", default=None, metavar="FILE",
                          help="Log messages to JSONL file (includes headers + body)")
    grp_disp.add_argument("--no-color", action="store_true",
                          help="Disable ANSI colors (for piping)")
    grp_disp.add_argument("-v", "--verbose", action="store_true",
                          help="Verbose output (server info, tracebacks)")

    # ── Meta ──
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    return parser


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.no_color:
        C.disable()

    # ── Send mode ──
    if args.send:
        send_single(args)
        return

    # ── Listen mode ──
    header_display = "all" if args.headers else ("minimal" if args.minimal else "key")

    print(f"""
{C.BOLD}{C.CYAN}╔═══════════════════════════════════════════════════════════════╗
║              OptimusDB EMS Client v{__version__}                      ║
║              STOMP Topic Consumer & Producer                  ║
╚═══════════════════════════════════════════════════════════════╝{C.RESET}
{C.DIM}  Host:         {C.RESET}{args.host}:{args.port}
{C.DIM}  User:         {C.RESET}{args.user}
{C.DIM}  Topic:        {C.RESET}{args.topic}
{C.DIM}  Client ID:    {C.RESET}{args.client_id}
{C.DIM}  Durable:      {C.RESET}{"yes" if not args.no_durable else "no"}
{C.DIM}  Headers:      {C.RESET}{header_display}
{C.DIM}  Topic filter: {C.RESET}{args.filter_topic or "none"}
{C.DIM}  Log file:     {C.RESET}{args.log or "none"}
""")

    conn, listener = create_connection(args)

    # Graceful shutdown
    def shutdown(signum=None, frame=None):
        print(f"\n{C.DIM}  Shutting down...{C.RESET}")
        try:
            conn.disconnect()
        except Exception:
            pass
        listener.close()
        print(f"{C.GREEN}  ✓ Disconnected. Received {_msg_counter} messages.{C.RESET}")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Main loop
    print(f"\n{C.DIM}  Waiting for messages... (Ctrl+C to quit){C.RESET}")
    print(f"{C.DIM}  Commands: [s]end  [c]ount  [h]elp  [q]uit{C.RESET}\n")

    try:
        while True:
            if select.select([sys.stdin], [], [], 1.0)[0]:
                line = sys.stdin.readline().strip().lower()
                if line == "s":
                    interactive_send(conn, args.topic)
                elif line == "q":
                    shutdown()
                elif line == "c":
                    print(f"{C.DIM}  Messages received: {_msg_counter}{C.RESET}")
                elif line == "h":
                    print(f"""
{C.CYAN}  Interactive Commands:{C.RESET}
    s  — Send a message (prompted)
    c  — Show message count
    q  — Quit gracefully
    h  — Show this help
""")
                elif line:
                    print(f"{C.DIM}  Unknown command '{line}'. Type 'h' for help.{C.RESET}")
    except (KeyboardInterrupt, EOFError):
        shutdown()


if __name__ == "__main__":
    main()
