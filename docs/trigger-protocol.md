# Unix-datagram trigger protocol

Clients send requests directly or use the independent
[`retroft-trigger` Rust library](../crates/retroft-trigger/src/lib.rs).
The recorder and command-line client use filesystem Unix-domain datagram sockets
(`AF_UNIX`, `SOCK_DGRAM`). There are no connections, acknowledgments, or replies.

## Requests and delivery

Send exactly one UTF-8 JSON object in each datagram. Datagram boundaries provide
framing: there is no length prefix or required newline. A send queues the entire
datagram or none; stream-style partial-frame handling does not apply.
The default path is `/run/retroft.sock`. `--socket` changes it; the
sample systemd socket unit uses the same default path.

```json
{"source":"motor-control","description":"control deadline missed"}
```

| Field | JSON type | Meaning |
| --- | --- | --- |
| `source` | Nonempty string | Required component or service label, at most 256 UTF-8 bytes. Not authenticated. |
| `description` | String or `null` | Empty if omitted/null. At most 1024 UTF-8 bytes; stored in the trigger marker. |
| `end_timestamp_ns` | Decimal string or `null` | Processing-time endpoint if omitted/null; otherwise absolute `CLOCK_MONOTONIC` nanoseconds. |

The entire datagram must contain at most **2048 bytes**; JSON escaping counts.
Unknown/duplicate fields, invalid types/UTF-8/JSON, oversized datagrams, and
invalid endpoints are discarded without a reply. The `ack`
field is rejected, and clients must not supply sequence numbers, peer credentials,
`schema_version`, or occurrence timestamps. There is no wire-version negotiation.

`end_timestamp_ns` is a canonical unsigned decimal string in the u64 range:
`"0"` or digits without leading zero. Signs, fractions, exponents, and JSON
numbers are invalid. A past endpoint is allowed but cannot recover previously
consumed/overwritten history. A future endpoint must be within 24 hours of daemon
processing time. To request another 500 ms, calculate this host’s
`CLOCK_MONOTONIC + 500000000` and encode the result as a decimal string. Do not use
wall time, `CLOCK_MONOTONIC_RAW`, or `CLOCK_BOOTTIME`. Clients in another time
namespace must translate their offset into the daemon’s monotonic clock domain.

A successful send proves **local queuing only**. It does not confirm validation,
acceptance, disk submission, durability, or capture completion. Invalid requests
can be dropped after a successful send. The daemon assigns each accepted trigger
its sequence number and marker timestamp when processing the request, so queueing
and scheduling delays shift that marker relative to the application event.
The sequence appears in the capture, never in a socket reply. Retrying a request
can produce another trigger; there is no deduplication key.

Triggers processed during an unsealed capture extend its endpoint and retain
individual descriptions. Once a capture is sealed, processing waits for its final
synchronization; later requests start another capture. Consumed history is not
repeated across captures. Orderly shutdown finishes accepted captures; unread
requests have no acceptance guarantee.

## Nonblocking critical callers

Prepare the JSON and destination address during initialization, then attempt one
nonblocking `sendto` per trigger. An unconnected socket resolves the stored
pathname on each send, so it can address a replacement daemon socket without
reconnecting. Preparing the client does not require the recorder to be running.

```rust
use retroft_trigger::{NonblockingClient, TrySendError};
use std::path::Path;

// Noncritical setup: allocate and encode the frame and destination once.
let mut client = NonblockingClient::new(Path::new("/run/retroft.sock"), "motor-control")?;
let prepared = client.prepare("control deadline missed", None)?;

// Critical path: one send, no userspace allocation, automatic retry, or logging.
match client.try_send(&prepared) {
    Ok(()) => { /* Locally queued; acceptance is unknown. */ }
    Err(TrySendError::WouldBlock | TrySendError::Interrupted) => {
        // No datagram queued; record a small status for later handling.
    }
    Err(TrySendError::Socket(_)) => {
        // Report or handle outside the critical path; the client remains reusable.
    }
}
```

`NonblockingClient::new(socket_path, source)` validates and owns the source label.
`client.prepare(description, endpoint)` encodes it into each request and checks
the field and encoded size limits. Preparation allocates; sending does not. `None` uses receipt time and lets the frame be reused. A supplied
endpoint is fixed during preparation, not a relative interval refreshed on every
send; validation of the 24-hour limit happens later at the recorder.

`try_send` uses `MSG_DONTWAIT | MSG_NOSIGNAL`, makes no userspace allocation, and
never waits for socket capacity, retries, logs, closes the socket, or expects a
reply. `WouldBlock` and `Interrupted` mean no datagram was queued. Other socket
errors, including a missing endpoint or a daemon restart, do not poison framing
or the client; future sends can succeed when the pathname is available again.
A connected datagram socket can retain an obsolete endpoint after replacement;
the library uses unconnected `sendto` to resolve the current endpoint.

Use one writer per client. The mutable Rust reference serializes calls. Syscalls
still execute kernel work and scheduling can delay the caller: nonblocking mode
is not a hard execution-time or jitter guarantee. Do not wait for readiness or
retry in a critical thread. The library has no one-shot helper: callers explicitly
create and retain their client. The CLI creates a client, prepares one request,
and attempts a single send; it reports local queuing only. CLI `--source` defaults
to `retroft-trigger`.

## Restart and backpressure

The daemon reads requests after the frozen bank drains, when marker staging is
available. A pending sync does not prevent staging, but a sealed capture must
finish syncing before another request can start a new capture. Unread datagrams
remain in the kernel socket queue. Queue capacity is finite; nonblocking sends
can return `EAGAIN`/`WouldBlock` under sustained overload. There is no separate
accepted-trigger queue and no per-client connection limit.

Systemd socket activation is optional. When used, the socket unit owns the
endpoint independently of the daemon and keeps its queue alive across daemon
restarts while that socket unit remains active. Requests already removed from
the queue before a crash are not preserved by socket activation. Stopping the
socket unit or exceeding its finite capacity also defeats that guarantee.

Without socket activation, the daemon owns the endpoint. Its exit can lose
queued requests, and sends during downtime can fail with `ENOENT` or
`ECONNREFUSED`. An unconnected prepared client can still send future triggers
once the new daemon binds the same pathname; systemd is not required for this
reuse. Neither setup promises delivery of an unlimited flood.

## Sender identity

The socket defaults to filesystem mode `0600`; owner/group and parent-directory
permissions control who can send. There is no separate authentication handshake.
The recorder enables Linux `SO_PASSCRED` and receives credentials with each
datagram. Capture fields `peer_pid`, `peer_uid`, and `peer_gid` describe the
sender in the daemon’s namespaces, including when the socket was inherited
or transferred from another process. Clients must not send these fields in JSON.

`source` is a human label, often a service/component name, not authenticated
identity. The recorder also samples `/proc/<pid>/comm` as `peer_process_name`
when available. That name is best effort and can be missing, stale after renaming,
or affected by process exit/PID reuse. The recorder does not infer a systemd
service name; supply it explicitly as `source` when useful.

## Direct Python client

This standard-library example prepares a reusable unconnected sender. Python
and socket syscalls do not provide a hard real-time bound.

```python
import json
import socket

path = "/run/retroft.sock"
frame = json.dumps(
    {"source": "motor-control", "description": "control deadline missed"},
    ensure_ascii=False, separators=(",", ":"),
).encode("utf-8")
if len(frame) > 2048:
    raise ValueError("encoded trigger exceeds 2048 bytes")
client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
client.setblocking(False)

# Later: attempt once. Catch/report OSError outside the critical path.
client.sendto(frame, socket.MSG_DONTWAIT | socket.MSG_NOSIGNAL, path)
```

Validate source and description limits before encoding arbitrary caller input.
No client bind, connect, shutdown, acknowledgment read, or newline is required.
A successful `sendto` does not confirm that the daemon accepted the request.

## Finding triggers in Perfetto

The exporter places triggers on the **Recorder triggers and accounting** track.
For example, `Trigger 42 [motor-control]: control deadline missed` exposes the
source and description. Marker arguments carry `source`, `description`, and
available sender credential/process-name fields. The sender PID is diagnostic
annotation, not an assertion that the marker belongs to that process’s timeline.

Marker timestamps are monotonic processing times. Clock snapshots provide
piecewise boottime and approximate UTC correlation, including detected resume
changes. Their sampling limitations are documented in the
[clock snapshot format](format.md#clock-snapshot-records).

Implementation references: [daemon](../crates/retroft/src/daemon.rs),
[Rust client](../crates/retroft-trigger/src/lib.rs), and
[exporter](../crates/retroft-export/src/offline.rs).

During capture, trigger datagrams remain in the kernel socket queue until the
current frozen bank has drained. The recorder then admits up to 16 datagrams
per event-loop turn, subject to staging space, before deciding whether to swap
or finalize. Socket readiness still wakes the recorder while idle or waiting
between swaps. This avoids a receive syscall for every trace sub-buffer.
Linux's `net.unix.max_dgram_qlen` defaults to 10; nonblocking clients must handle
queue-full errors. The recorder does not change this system-wide setting.
