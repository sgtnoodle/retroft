# Tracefs backend contract

The recorder creates an exclusive named instance under `tracefs/instances`.
It sets the `nop` tracer, disables inherited events, enables overwrite mode,
configures the buffer geometry and clock, preserves enabled event formats,
preallocates a spare snapshot bank, and opens one nonblocking `snapshot_raw`
descriptor per CPU before starting tracing. No raw read occurs before a capture trigger. Snapshot support is required
(`CONFIG_TRACER_SNAPSHOT`); startup fails clearly when it is unavailable.

The root must be an actual tracefs mount, or the legacy debugfs tracing mount.
An inaccessible mount or configuration file produces an explicit startup
error. By default an existing instance is never adopted or removed; after
SIGKILL, inspect and remove that stale instance or use a new name. For automatic
restart, pair a fixed `--instance` with `--instance-owner PATH`, as the sample
service does. This regular file holds an exclusive lock and a persistent marker
reserving the canonical instance path. A matching marker permits stopping and
removing a crashed instance before creating a fresh one; unmarked or mismatched
instances are refused. The intent marker is written before instance creation so
process death cannot leave a newly created instance without its ownership record.
The reservation persists across clean shutdown; never reuse its instance for
another tool or remove the lock file while a recorder holds it.
Orderly shutdown disables this instance's events, closes readers,
and removes the instance. Unrelated tracefs instances and eBPF programs are not
modified.

The recorder fixes the CPU set to CPUs online at startup. Restart after CPU
hotplug. Suspend and CPU-hotplug behavior still need live platform testing.

## Snapshot and read behavior

Each CPU has two equally sized kernel rings. A trigger swaps live and spare
banks while tracing remains enabled. The old bank is drained through
`per_cpu/cpuN/snapshot_raw`; the other bank collects new events. The spare is
allocated before tracing starts, avoiding allocation on the trigger path.
No mmap mapping is created: the kernel disallows snapshot swaps while mapped.
There is no live-reader fallback or configurable backend.

The first trigger creates the capture file, writes its header, submits an
asynchronous directory sync, and stages initial metadata before swapping.
This leaves filesystem preparation latency in the overwriting history window
rather than spending the new live bank's headroom. After swapping, it invokes
`MEMBARRIER_CMD_GLOBAL` before
reading. This relies on the Linux implementation waiting for an RCU grace period.
The recorder consumes every CPU's frozen data before swapping again. Consuming
reads leave the bank reusable, including partially consumed reader pages; there
is no explicit clear or settling timer. Cumulative counters travel with each bank,
and raw loss notifications remain relative to its consuming reader.
Completion requires a swap whose immediately preceding monotonic time reaches
the requested endpoint, followed by the barrier and draining its bank. Whole valid
sub-buffers and overshoot events are preserved. Extensions retain their markers
and increase the endpoint; they never return an unread bank to production.

`CONFIG_MEMBARRIER` and `MEMBARRIER_CMD_GLOBAL` support are required; `nohz_full`
is incompatible. No timed fallback is used. See [kernel notes](kernel-notes.md#snapshot-banks-and-capture-completion)
for the implementation dependency and blocking behavior.

The reader requests exactly one configured sub-buffer allocation at a time.
Kernel raw reads can return a partially committed writer page as a full,
zero-padded allocation. Only the header's committed data contains events.
Empty reads return EAGAIN or zero; neither is a valid page. Rare short reads
are assembled in preallocated per-CPU memory, preserving assembly across
interruption and temporary unavailability. Raw reads give the recorder its own
copy of each page. The offline decoder also supports the committed/skip boundaries in older mmap captures.

Frozen data cannot be overwritten by new live-bank events. A simple CPU order
is sufficient; no weighted scheduler or production-rate estimate is needed.
During stored-byte batch synchronization, reads and compression can continue
into the fixed staging pool. They pause when that pool fills. Storage or
compression stalls can still let the accumulating live bank wrap.
Two banks provide more time to drain, but do not guarantee loss-free recording
or a fixed swap latency. See the
[kernel snapshot documentation](https://www.kernel.org/doc/html/latest/trace/ftrace.html#snapshot)
and [snapshot/raw read implementation](https://github.com/torvalds/linux/blob/master/kernel/trace/trace.c).

## Clock, metadata, and accounting

Initial-history trimming probes the first two event-bearing pages of each frozen
CPU (or stops at EOF), retaining two pages per CPU in startup-allocated memory.
An overwrite on the second page discards the preceding retained reader fragment
and establishes the boundary from the second page. Without loss, both pages are
preserved for normal cutoff processing. Empty CPUs do not
constrain the window and their later arrivals are preserved. Control-only
pages are written unchanged; a leading loss flag on one also applies when
choosing the following event-bearing page's history boundary.

Only confirmed leading loss can raise the common start. Silence alone is
valid history, including after a recent capture. The cutoff is the maximum
loss-backed first event, capped at the initial swap timestamp, minus the
configured margin (default one second). Without such a loss, no trimming is
performed. The cutoff stays fixed through extensions. Trimming discards only
whole initial-prefix pages whose maximum actual event timestamp precedes the
cutoff, never a page merely because its base timestamp is old. Crossing pages
and all subsequent snapshot banks are preserved. `--keep-leading-history`
disables probing and trimming.

The daemon uses the tracefs `mono` clock. Linux maps `mono` to
`ktime_get_mono_fast_ns`, in the CLOCK_MONOTONIC domain used by trigger
endpoints. It is distinct from `mono_raw` / CLOCK_MONOTONIC_RAW: `mono`
includes normal timekeeping adjustments and excludes suspended time.
Tracefs `boot` is CLOCK_BOOTTIME, including suspended time. The backend
library can select boot, but callers then must convert trigger endpoints;
the recorder API requires `mono`.

Startup metadata samples CLOCK_MONOTONIC around a CLOCK_BOOTTIME sample and
records the midpoint, boottime value, and sampling interval as decimal-string
nanoseconds. Offline export uses this relationship for Perfetto's boottime
domain for older captures lacking clock-snapshot records. Captures with these
records include fresh CLOCK_MONOTONIC/CLOCK_BOOTTIME/CLOCK_REALTIME observations and
retained suspend-offset history as record kind 7. The recorder monitors
boottime-minus-monotonic roughly once per second and stores observations
around detected changes while keeping the capture and tracing continuous.
It does not record every periodic sample or roll/flush files on a clock change.

Offline export prescans snapshots, extends the first mapping backward to
earlier trace history, and uses piecewise clock relationships thereafter.
Native Perfetto clock snapshots provide approximate UTC correlation using
the host realtime clock. Early resumed events before the next monitor sample
still belong to the previous sampled correlation epoch. Monitor delays and multiple sleep cycles
between samples prevent an exact resume timeline; sampling uncertainty is
not a bound on that ambiguity. NTP steps are not independently logged each
period. Snapshot-aware exports keep the primary scheduling timeline monotonic;
the previous observation affects early-resume UTC/boottime correlation, not
original monotonic event times. See [clock snapshot records](format.md#clock-snapshot-records).

Raw `header_page`, `header_event`, and enabled event `format` files are
preserved unchanged and bounded to 8 MiB total. Kernel word size and header
offsets come from `header_page`, supporting architecture-dependent alignment
without native-struct assumptions.
Process/thread names come from a best-effort bounded `/proc` snapshot before
tracing begins. Exits, forks, and renames can race that snapshot; captured
lifecycle events supplement it. At most 65,536 initial threads are retained,
and truncation is disclosed.

Raw-read records retain the commit word's missed-event flags and optional
stored loss count. An unknown count is recorded as unavailable, not zero.
Per-CPU tracefs stats include overwrite, commit overrun, dropped events, bytes, and read
events as available. Counter values use decimal strings and their original
stats text is also retained. These statistics refer to the live bank; bank
swaps and recycling can reset or change their counters. Start/end differences
therefore do not measure losses in the capture. Native raw missed-event markers
are authoritative for detected kernel loss.

Initial-bank loss is expected rolling-history overwrite and does not warn.
Loss reported in later banks warns once per CPU per capture; native markers
and counters remain in the file in both cases. The exporter can clean leading
history by default or retain all decodable information with `--raw`.
Capture accounting reuses stats descriptors opened at startup and serializes
borrowed counter fields directly into bounded caller memory.

Memory accounting includes both snapshot rings, their reader pages, raw-read
kernel overhead, staging, and userspace short-read accumulators. Values are
estimates excluding kernel allocator bookkeeping and other runtime memory.
`--buffer-per-cpu` specifies each bank's size, so ring allocation is approximately
twice the sum of the configured per-CPU sizes.

Unit tests exercise raw layouts, loss flags, reading, and capture state changes.
Live repeated snapshots, late-writer handling, Perfetto import, suspend, ARM
behavior, jitter, and real power-loss durability must be measured on targets.

During post-trigger collection, after draining the frozen bank, the recorder
checks the active bank's unconsumed `bytes` every 100 ms. It swaps when any CPU
reaches 50% of that CPU's configured buffer capacity, or when the trigger endpoint
arrives, regardless of occupancy. The interval and threshold are fixed. Checks use
the existing statistics descriptors and do not consume events. There is no extra
polling while idle. The 50% headroom reduces overrun risk but cannot guarantee
against bursts or a slow barrier/drain; a final swap can also be delayed while
the preceding bank is still draining.

The first post-trigger wait logs its initial drain duration. Timing begins after
the first swap and writer barrier return and ends when all frozen CPUs have
been consumed. Pending storage writes need not have completed at that point.
The log is emitted after queued-trigger admission and the swap decision, and
is omitted if there is no post-trigger wait.
