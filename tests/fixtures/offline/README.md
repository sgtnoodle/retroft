# Independent raw-page fixtures

These synthetic pages were constructed directly with Python `struct.pack`,
following Linux `include/linux/ring_buffer.h`, not through the Rust encoder.
`little64.page` represents x86-64/aarch64; `big32.page` exercises big-endian
32-bit bitfields and native `long`. Page size is 4096. Bytes after the committed
region and appended loss counter are `0xa5`, to catch accidental tail decoding.

Both pages contain, in ring order:

1. sched_switch, idle PID 0 to worker PID 42, state 1, timestamp 1010.
2. TIME_EXTEND (high word 1, low delta 3), then sched_waking worker PID 43,
   target CPU 1, timestamp 134218748.
3. TIME_STAMP (high word 2, low word 2000), then extended-length sched_switch,
   PID 42 to PID 43, signed prev_state -1, timestamp 268437467.
4. Discarded padding (delta 2, length 8), then sched_switch PID 43 to PID 0,
   timestamp 268437474.

The little-endian committed region is 276 bytes; the big-endian region is 264
bytes. Both have MISSING_EVENTS and MISSING_STORED set and exact loss count 9
immediately after their committed regions. Their base timestamp is 1000.

Independent validation can use installed libtraceevent with
`tests/offline_libtraceevent_check.py`; it compares actual kbuffer timestamps,
event IDs, payload lengths, and kernel loss counts against those expectations.

ABI/protobuf sources:

- https://github.com/torvalds/linux/blob/master/include/linux/ring_buffer.h
- https://github.com/rostedt/libtraceevent/blob/master/src/kbuffer-parse.c
- https://github.com/rostedt/trace-cmd/blob/master/lib/trace-cmd/trace-input.c
- https://github.com/google/perfetto/tree/main/protos/perfetto/trace/ftrace

The fixtures exercise ABI mechanics rather than claim to be real kernel captures.

Native import validation was also run with the official Perfetto v58.2 Linux
trace processor (SHA256
`58042408e6cc861fb1a731c26bb082dc222285561eaa4e12a48a8b2b90dca7b9`).
The Rust fixture adds a recorded 500 ns MONOTONIC-to-BOOTTIME offset. Both
architectures produced scheduling rows at 1510 (PID 42), 268437967 (PID 43),
and 268437974 (idle), plus the trigger at 2500. PID 43's runnable latency was
134218719 ns. Perfetto reported the deliberately injected CPU loss and no
parser error statistics.

To repeat with an existing official `trace_processor_shell`:

```sh
RETROFT_TEST_ARTIFACT_DIR=/tmp/offline-artifacts cargo test -p retroft-export --lib native_perfetto_export
python3 tests/offline_perfetto_check.py /path/to/trace_processor_shell /tmp/offline-artifacts
python3 tests/offline_libtraceevent_check.py
```

The exporter accepts the modern 5-bit type/27-bit delta layout and target
word sizes 4 and 8. It supports mono and boot trace clocks; unsupported clocks
fail clearly because no trustworthy conversion was recorded. Startup clock
correlation in legacy files assumes no intervening suspend; clock-snapshot records
provide sampled correlations for newer captures. Scheduling, wakeups, lifecycle,
and IRQ/softirq have native event messages. Named syscalls also produce native
duration slices on supported architectures. Original syscall fields and other
enabled events remain available as generic ftrace messages: integer fields retain
signedness and binary arrays are shown as hexadecimal bytes. Lock events remain
inspectable raw events without dedicated contention slices. Unknown event IDs are omitted
with explicit per-ID counts. Malformed committed pages fail export with the CPU
and record number; a checksum-invalid or truncated container tail instead
exports its preceding valid records and reports incomplete recovery. Existing
output paths are refused. Per-CPU durations cover recovered event timestamps;
they do not prove endpoint coverage on a quiet CPU.
