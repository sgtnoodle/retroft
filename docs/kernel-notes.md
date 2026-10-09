# Kernel ABI investigation

These notes describe upstream kernel implementations and the interfaces Retroft
checks at startup. Source inspection explains behavior but does not replace
runtime validation on the target kernel.

## Capability checks

Required kernel features include tracing, ring buffers, tracepoints, ftrace,
and snapshot support. Retroft checks the tracefs instance interfaces at startup.
Kernel configuration or header presence alone does not establish that those
interfaces are accessible or operational.

## Raw reads

`tracing_buffers_read` obtains one sub-buffer with `ring_buffer_read_page(..., full=0)`, then serves its bytes through an fd-local spare. Request exactly the discovered sub-buffer size. A successful read normally returns that entire size, including padding, even when only part contains events. A small request can split this framed page across reads; preserve fd-local assembly state after a short read. Empty nonblocking reads return `EAGAIN`, while certain no-transfer cases return zero. Do not classify zero as a valid page. No special userspace pointer alignment is imposed by this path. [Upstream Linux 6.12 implementation](https://raw.githubusercontent.com/torvalds/linux/v6.12/kernel/trace/trace.c)

The [Linux 6.1-rc2 implementation](https://github.com/torvalds/linux/blob/v6.1-rc2/kernel/trace/trace.c) uses `PAGE_SIZE`; newer implementations discover the configured sub-buffer size. The [ring-buffer implementation](https://github.com/torvalds/linux/blob/v6.1-rc2/kernel/trace/ring_buffer.c) copies complete committed events out of an unfinished writer page, updates the returned base timestamp, zeroes padding, and encodes missed-event flags/counts. Idle CPUs therefore do not require a writer to fill a whole page before the read backend can preserve their committed events.

## Snapshot banks and capture completion

The ftrace snapshot interface swaps the live ring with its spare while tracing
continues. A spare must be allocated before tracing starts. Per-CPU
`snapshot_raw` reads consume frozen data in the native ring-buffer format.
Writers already in flight at the swap can finish against the previous bank.
After each swap, Retroft invokes `membarrier(MEMBARRIER_CMD_GLOBAL, 0, 0)` before
reading. Current Linux implements this with `synchronize_rcu()` when more than
one CPU is online; the single-CPU path returns directly. The normal ftrace writer
critical sections are covered by this grace period. Once synchronized, consuming
raw reads drain the bank for reuse without clearing it or waiting on a timer.

This relies on the Linux implementation: the documented membarrier
ABI guarantees userspace memory ordering, not a general kernel-reader barrier.
Future kernel changes to that implementation need review. `CONFIG_MEMBARRIER`
and the GLOBAL command must be available; `nohz_full` disables it. Retroft checks
support before creating the instance and aborts on a barrier failure. Expedited
commands are not substitutes. The manifest records
`snapshot_writer_barrier: "membarrier_global"` for this policy.

The barrier blocks the recorder and may generate RCU housekeeping; its latency
is not bounded. Kernel settings can also request expedited RCU behavior. The live
bank continues accumulating data during the wait and can overflow. CPU hotplug
remains unsupported during a recorder session.
[Kernel snapshot documentation](https://www.kernel.org/doc/html/latest/trace/ftrace.html#snapshot),
[membarrier implementation](https://github.com/torvalds/linux/blob/master/kernel/sched/membarrier.c),
[membarrier API](https://man7.org/linux/man-pages/man2/membarrier.2.html).

Kernel mappings prohibit snapshot swapping; Retroft uses raw reads. Older captures with `committed_bytes` and
`skip_bytes` envelopes remain supported by the exporter. Their recorded
frontiers remain authoritative over a potentially growing native header.
[Kernel mmap limitations](https://www.kernel.org/doc/html/latest/trace/ring-buffer-map.html#limitations).

Select `mono` and use CLOCK_MONOTONIC nanoseconds for client endpoints.
The kernel maps `mono` to `ktime_get_mono_fast_ns`; this shares the monotonic
reference but can briefly move backward during timekeeper updates. Monotonic
time excludes suspend. Record this relationship and do not claim strict event
ordering across such anomalies.
[Timekeeper implementation](https://github.com/torvalds/linux/blob/master/kernel/time/timekeeping.c).

The endpoint is reached by taking a snapshot after the immediately preceding
monotonic clock observation reaches it. Drain the whole bank and preserve
overshoot. Writer synchronization does not prevent live-bank overwrites. Trigger extensions can require another bank swap after
the previous frozen bank has drained. Kernel loss markers and actual decoded
event bounds remain the evidence of available history. Statistics sample the
live bank, whose counters can change or reset across swaps and recycling;
start/end subtraction cannot establish capture loss.

## Sampled suspend/resume clock correlation

The recorder keeps tracing and capture files continuous across detected
boottime-minus-monotonic changes. Kind-7 records sample CLOCK_MONOTONIC,
CLOCK_BOOTTIME, and CLOCK_REALTIME at file creation and around detected changes.
An approximately one-second monitor retains its latest observation and
offset-change history, rather than recording every periodic sample. It does
not roll or force a file sync solely because the offset changed.

Snapshot-aware export uses piecewise clock relationships and emits native
Perfetto clock snapshots with approximate UTC correlation. The primary event
timeline remains monotonic, preserving scheduling durations in that domain;
legacy files without snapshot records retain their previous boottime mapping.
The post-resume observation is not an exact wake timestamp. Early resumed
events before that observation retain the old correlation, and scheduling
delays can enlarge the ambiguity with no hard upper bound. Multiple sleep
cycles between observations or lost/overwritten events cannot be reconstructed
exactly. Clock-read uncertainty does not bound this detection gap or the host
realtime clock's accuracy. The monitor uses a 1 ms offset-change tolerance
plus the before/after sample uncertainties and skips uncertainty above 1 ms
for baseline replacement. It can miss short sleeps. Before/after pairs are retained
without a cap since daemon startup,
while unchanged periodic observations only replace the last usable sample.
Ordinary NTP/realtime steps are not independently
recorded each monitoring period. See [kind-7 schema](format.md#clock-snapshot-records).

## Validation status

Live user-supplied x86-64 captures have exercised first/repeated swaps,
consecutive capture boundaries, idle CPUs, 64 KiB sub-buffers, compression,
named syscalls, and storage batch syncing. The 2026-10-07
[performance assessment](performance.md) records two complete, loss-free
captures and distinguishes recorder drain time from snapshot/barrier latency.

Target validation is still required for worst-case application jitter, live
ARM32/AArch64 and PREEMPT_RT kernels, suspend/resume, late-writer stress,
sustained storage backpressure, and actual power-loss durability. CPU hotplug
is unsupported and requires a restart. Unit tests of foreign raw layouts and
file truncation do not establish these runtime properties.
