# Retroft design

## Purpose and constraints

Retroft keeps recent kernel execution history in ftrace's per-CPU overwrite
buffers and writes that history to disk when triggered. Tracing continues during
capture. Event decoding and Perfetto export run offline, so analysis dependencies
are not needed on the recording system.

The recorder should consume spare resources without disrupting the application
being diagnosed. Losing old diagnostic history is preferable to unbounded memory
use or blocking a critical trigger client. The implementation should remain small
and readable; modest allocations for control metadata are acceptable.

The target architectures are x86-64, AArch64, and ARM32, with or without PREEMPT_RT.
One build per architecture should work across kernels that provide the required
interfaces. Startup checks capabilities and discovers raw layouts from tracefs;
it does not require matching kernel headers or an exact kernel release.
Live validation covers x86-64. The other targets require platform testing.

The history target is at least 20 seconds, with a soft low-end memory budget of
about 32 MB for kernel buffers and the recorder together. A representative system
has 4 GB of RAM with about 2 GB free. These are sizing goals, not defaults or
limits: history depends on event volume, and the operator chooses the memory
budget. Both snapshot banks count toward kernel memory use.

The added worst-case jitter target for critical tasks is below 250 microseconds.
That requires measurement on the target workload and platform; it is not a
portable guarantee. The recorder inherits scheduling priority from its launcher.
The supplied systemd service uses normal scheduling with nice +19.

## Components

The Cargo workspace separates recording from offline analysis:

| Crate | Responsibility |
| --- | --- |
| `retroft` | Recorder library and daemon CLI. |
| `retroft-format` | Shared record headers, framing, and CRC32C writer. |
| `retroft-trigger` | Nonblocking trigger client and CLI. |
| `retroft-export` | Raw decoder, Perfetto exporter, and capture validator. |

The recorder is the default workspace member. Building it does not build the
exporter. LZ4 is an optional feature enabled by default; builds without it record
uncompressed data and reject requests to enable compression.

The recorder uses tracefs directly and the Rust `io-uring` crate for storage.
It needs no async runtime, liburing, libtracefs, or libtraceevent. One thread owns
capture state and storage submissions. Unsafe code at operating-system interfaces
must document buffer lifetimes, alignment, and ownership.

`Recorder::step` fits into a caller's event loop. `daemon::run` wraps it in a
blocking loop suitable for a main thread or dedicated worker. The caller supplies
a trigger source and a status callback. The CLI adds the Unix socket, signal
handlers, console output, and systemd readiness notification. See the
[embedding guide](docs/embedding.md) for integration examples and workspace setup.

## Tracing and snapshot banks

Startup creates an exclusive named tracefs instance, selects the `nop` tracer
and `mono` clock, configures buffers and events, and allocates a spare snapshot
bank. Each CPU has two equally sized rings. The recorder opens nonblocking
`per_cpu/cpuN/snapshot_raw` descriptors before enabling tracing. It does not
consume raw data while idle.

The default events cover scheduling and process lifecycle. `--syscalls` adds
named syscall entry/exit events, and `--irqs` adds available hard-IRQ and soft-IRQ
events. Explicit required and optional event lists allow other selections.
High-volume event classes need larger buffers or shorter history expectations.

A trigger prepares the file and initial metadata, then swaps the live and spare
banks. The old bank is frozen for reading while the other records new events:

```text
tracepoints -> live per-CPU rings
                    |
               snapshot swap
                    |
             frozen per-CPU rings -> raw reads -> compression -> staging -> disk
```

After each swap, `MEMBARRIER_CMD_GLOBAL` waits for pre-swap writers before the
recorder reads the frozen bank. This depends on Linux's implementation using
`synchronize_rcu()`, beyond the syscall's documented userspace ordering contract.
The barrier may block for an unbounded time while the live bank continues filling.
Snapshot and GLOBAL membarrier support are required; there is no timed or
live-reader fallback. Kernel mappings prohibit snapshot swaps, so the recorder
does not use mmap. See the [kernel notes](docs/kernel-notes.md).

Each turn reads at most one sub-buffer, visiting CPUs in round-robin order.
Frozen data cannot be overwritten by ongoing tracing, so a weighted CPU scheduler
is unnecessary. Short reads are assembled in preallocated per-CPU memory. Empty
reads are not valid pages. A bank must be fully consumed before it can be reused;
consuming reads make it reusable without an explicit clear.

If the capture needs more data, the recorder checks live-bank occupancy every
100 ms after draining. It swaps when any CPU reaches 50% of its configured
capacity, or when the requested endpoint arrives. A final snapshot must be taken
after the endpoint and drained before the capture can close. Whole sub-buffers
are retained, including events beyond the endpoint.

Two banks provide time to drain but cannot absorb an unlimited storage or CPU
stall. A busy live bank can wrap while the frozen bank remains safe. Buffer sizes
must cover expected production during swaps, barriers, and draining.

## Trigger handling

Each Unix datagram contains one UTF-8 JSON request with a required source label,
an optional description, and an optional absolute CLOCK_MONOTONIC endpoint.
The recorder assigns the trigger sequence and timestamp when it accepts the
request. Queueing and scheduling delays therefore affect the marker time.
A missing endpoint means the time of acceptance; a future endpoint requests
post-trigger data.

There are no replies or acknowledgments. A successful send confirms local
queueing, not acceptance or durable capture. Critical clients prepare requests
outside their critical path and attempt one nonblocking send. The client library
allocates nothing, waits for no socket capacity, and performs no retries during
that send. An unconnected socket resolves the pathname each time, allowing reuse
after a standalone daemon restarts. Kernel work and scheduling can still delay
the syscall. See the [trigger protocol](docs/trigger-protocol.md) for limits.

The recorder leaves requests in the socket until the frozen bank drains. It then
processes up to 16 requests per turn, subject to staging capacity, before deciding
whether to swap or close. Each accepted marker goes directly into staging;
there is no separate accepted-trigger queue. Socket queues are finite, so
nonblocking clients must handle full-queue errors.

Accepted triggers extend the active endpoint to the maximum requested time and
retain their individual markers. Once End is submitted, that file is sealed.
Later requests wait for its final sync and start another capture. Consumed events
are not captured again. Consecutive files can be exported together when their
recorded session and predecessor information establishes continuity.

Linux credentials identify the sender of each datagram, including through an
inherited socket. The source label is client-supplied, and a sampled process name
is diagnostic information. Neither label is an authenticated service identity.

## Event loop and shutdown

Each turn reaps storage completions, drains a page, accepts eligible triggers,
and then closes the capture or swaps another bank. Closing has three states:

```text
draining -> End submitted, waiting for writes -> waiting for final sync
```

Final accounting and End share one staging submission. Completion is reported
only after outstanding writes, the final file sync, and the creation-time
directory sync succeed. Reusable compression and probe state is reset after
completion so the next trigger can use it immediately.

When no work is ready, the recorder waits on one epoll descriptor. It combines
trigger, storage, shutdown, and timer readiness. A timerfd serves clock sampling,
post-trigger checks, and idle reports. Trigger readiness is disabled while
admission is blocked, preventing unread requests from causing a busy loop.

Shutdown wakes the loop, stops trigger admission, shortens any future endpoint,
and finishes accepted capture data. It then disables tracing, closes readers,
and removes the owned instance. The library installs no process signal handlers.
Dropping an active recorder attempts cleanup but does not guarantee a completed
capture. SIGKILL and kernel crashes cannot run cleanup.

A locked `--instance-owner` file reserves a stable instance name and permits
cleanup after a crash. Without matching ownership, startup refuses an existing
instance. Unrelated tracefs instances and eBPF programs are left alone.
Optional systemd socket activation preserves unread queued requests across daemon
restarts; it cannot preserve requests already dequeued before a crash.

## History trimming and loss

The recorder probes the first two event-bearing pages of each CPU in the first
frozen bank, stopping at EOF. A retained reader fragment can precede an overwrite
marker on the second page; that fragment is discarded when loss is confirmed.
The probe uses two sub-buffers per CPU, allocated at startup.

Only a leading loss marker establishes a history cutoff. Quiet CPUs and empty
buffers do not move it. The cutoff is the latest first surviving event backed by
loss, capped at the initial swap time, minus the configured margin (one second
by default). Without leading loss, there is no trimming.

The cutoff stays fixed through trigger extensions. Only whole leading pages
whose events all precede it are discarded. Crossing pages, control-only pages,
and later snapshot banks are retained. Accounting records discarded pages, bytes,
and loss markers separately. The report prints after the first bank drains;
offline export cannot restore discarded data.

Raw missed-event markers are the evidence of detected kernel loss. Live-bank
statistics can change or reset across swaps, so subtracting start and end counters
does not measure capture loss. Initial-bank overwrites are expected history
eviction. Later-bank loss produces one warning per CPU per capture. Unknown loss
counts stay unknown; they are not reported as zero.

## Memory, compression, and storage

Staging blocks have fixed addresses and are allocated at startup. Their size
fits the initial metadata/accounting/trigger batch and the largest trace or
metadata record. A block cannot be reused until its write completes. Pool memory
is bounded by `--staging`; accounting scratch, cached metadata, compression state,
and kernel buffers are reported separately.

Linked LZ4 compression runs on the recorder thread and reuses allocated workspace.
History follows trace-record order across CPUs, including raw fallback records.
Metadata stays uncompressed. A reset occurs before a record would exceed the
configured group size, normally 4 MiB of uncompressed trace data. An oversized
record forms its own group. LZ4 uses at most the preceding 64 KiB as its dictionary.
Reset records allow recovery to resume after corruption without guessing history.

Storage writes through io_uring in file order. At the stored-byte batch threshold
(default 8 MiB), it finishes the batch's writes and short-write retries, then
syncs before submitting the next batch. Reading and compression can continue into
free staging blocks during sync. When the pool fills, draining pauses; it does
not allocate more buffers.

After a successful sync, `POSIX_FADV_DONTNEED` asks the kernel to discard durable
whole pages from cache. This is advice, not a hard page-cache bound. A partial
boundary page remains for later advice. There is no periodic sync timer: a small
batch can remain unsynced until it reaches the threshold or the capture ends.

File creation, header writing, and directory-sync submission precede the first
swap. Their latency therefore spends rolling history instead of live-bank drain
headroom. A final file sync is required before reporting completion; no second
directory sync is needed. Disk-full and write/sync errors terminate capture and
return an error. There is no automatic file deletion, quota, or rotation policy.

## Format and clocks

The append-only `.retroft` container stores fixed-width little-endian headers,
raw ftrace sub-buffers, and versioned JSON metadata. Headers and stored payloads
are covered by CRC32C. Raw pages keep the recording kernel's byte order and ABI;
tracefs header and event-format files are saved unchanged for offline decoding.
Each file includes its own decoding metadata and needs no sidecar.

JSON timestamps, sequence numbers, and 64-bit counters use decimal strings to
preserve their values in tools with floating-point JSON numbers. Trace payloads
remain binary. Lengths, codecs, flags, and required schema versions are validated
before decoding. A torn tail leaves a valid prefix; stream corruption permits
recovery only at a later verified compression reset. Metadata corruption is fatal.
A valid End means logical completion, not proof that the final sync succeeded.
See the [format specification](docs/format.md) for layouts and recovery rules.

Tracing and trigger endpoints use CLOCK_MONOTONIC, which excludes suspend.
Clock snapshots correlate it with CLOCK_BOOTTIME and CLOCK_REALTIME at file
creation and around detected resume offsets. A roughly one-second monitor keeps
its latest sample and before/after pairs for offset changes. That history grows
with detected changes, without a configurable cap; ordinary samples replace the
previous observation. Tracing and capture files continue through suspend.

The exporter preserves monotonic event times and supplies approximate UTC
correlation. Early resumed events may use the preceding clock mapping until a new
sample is taken. Sampling uncertainty does not bound that delay, unobserved
suspend cycles, or realtime clock adjustments. Process identity snapshots also
race process changes. Export summaries list these general caveats as limitations;
warnings describe issues identified for the export.

## Compatibility and validation

Development formats and APIs may change before release. After release, exporters
must read every released container version; recorders emit the latest. Breaking
changes to framing, record kinds, codecs, flags, or required metadata semantics
bump the container version. Optional fields need documented defaults. Preserve
fixtures and export tests for each released version.

Tests cover raw layouts, framing and corruption recovery, compression history,
trigger behavior, capture boundaries, and storage ordering. Live x86-64 captures
exercise snapshots, syscall and IRQ export, and consecutive-file analysis. The
[performance assessment](docs/performance.md) separates measured drain time from
snapshot and barrier latency.

Platform validation must cover worst-case application jitter, ARM32/AArch64 and
PREEMPT_RT, suspend/resume, late writers, sustained storage pressure, and actual
power loss. CPU hotplug requires a recorder restart. Unit tests and successful
sync calls cannot establish device-cache durability or kernel timing bounds.
File preallocation, memory locking, and other optimizations need measurements
before adding complexity. Keep decoding and analysis in the offline tools.
