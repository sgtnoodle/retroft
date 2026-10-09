# Capture container version 1

The file starts with the 16 bytes `FTRREC01`, little-endian u32 version 1,
and four zero reserved bytes. It then contains consecutive records. All
container integers are little-endian; raw ftrace payloads retain the target
kernel's byte order and word size.

`FTRR` is the ftrace record marker; `FTRREC01` identifies the Retroft
container. These are format signatures, not application-domain names.

Each record has a 64-byte header:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | ASCII `FTRR` |
| 4 | 2 | Record format version, 1 |
| 6 | 2 | Kind: manifest=1, original metadata file=2, trace=3, trigger=4, accounting=5, end=6, clock snapshot=7 |
| 8 | 4 | Codec: raw=0, linked LZ4 block=2; codec 1 unsupported |
| 12 | 4 | CPU ID; `0xffffffff` for non-CPU records |
| 16 | 8 | File record sequence, starting at 1, no gaps |
| 24 | 8 | Timestamp in nanoseconds; trace page base or monotonic trigger receipt time |
| 32 | 4 | Stored payload length |
| 36 | 4 | Uncompressed payload length |
| 40 | 4 | Authoritative committed raw data bytes, excluding page header |
| 44 | 4 | Already consumed raw data bytes, excluding page header |
| 48 | 4 | Flags: bit 0 resets trace compression history; other bits unsupported |
| 52 | 4 | Reserved, zero |
| 56 | 4 | CRC32C |
| 60 | 4 | Reserved, zero |

CRC32C covers the entire header with its CRC field zeroed, followed by the
stored payload. Both lengths are bounded to 16 MiB. Only trace payloads can
be compressed. Codec 2 stores a raw LZ4 block, without an LZ4 frame header;
its decoded size comes from the envelope. Raw trace records can be mixed
with compressed records and participate in the same history. Non-trace
records must use codec 0 and zero flags. See the linked-history rules below.

## Linked LZ4 history

Compression history follows **trace record file order across all CPUs**, not
per-CPU order. Metadata, trigger, accounting, clock snapshot, and end records
neither update nor reset it. In a compression-enabled capture, the first trace record
sets flag bit 0. Raw-only captures need no reset markers. A reset clears preceding history before decoding that record.
Both raw and LZ4 trace records contribute their entire uncompressed payload
(including the raw page header and padding), regardless of committed/skip
fields. LZ4 references at most the preceding 64 KiB of that history.

`--compression-reset` bounds each group by uncompressed trace bytes, default
4 MiB. The recorder resets before a record that would exceed the budget;
record boundaries are preserved. A larger individual record forms a standalone
group and the next trace record starts fresh. The stored payload uses codec 0
when compression would not reduce its size, and this raw fallback still
advances history. Reset flags remain meaningful on raw fallback records.

Decoders must process intervening raw records and retain history before
reading a linked block. An explicitly reset record can be decoded without
preceding trace payloads, providing a bounded restart point. A torn tail
still leaves the preceding checksum-valid prefix recoverable. After corruption,
recovery may resume only at a checksum-valid trace record with the reset flag,
never at dependent blocks with guessed history. The exporter scans for that
restart point only after the metadata prefix has ended. Codec 1 from development-only
Zstandard captures is rejected rather than supported as a legacy format.

Manifest, trigger, accounting, end, and clock snapshot payloads are UTF-8 JSON objects with
`schema_version: 1`. Timestamps, sequence IDs, and 64-bit accounting values
are decimal strings. Unknown optional JSON fields can be ignored. Unsupported
required schema/container versions fail decoding. Missing optional fields use
their schema-defined defaults.

Snapshot captures record `snapshot_writer_barrier: "membarrier_global"`.
This means every bank swap was followed by the Linux GLOBAL membarrier before
consuming reads, and drained banks were reused without clearing. Older captures
lack this optional policy field. It describes writer synchronization, not absence
of ring overwrites; see [kernel notes](kernel-notes.md#snapshot-banks-and-capture-completion)
for the implementation dependency.

Manifests record `sync_batch_bytes` as a decimal string: the recorder's
stored-byte batch target, including outer framing, before ordered synchronization.
A complete indivisible write may exceed the target. Older `sync_interval_ns`
fields remain readable as legacy metadata; exporters do not use either field to
infer durability. Collection policy changes do not change the container version.

An original-metadata-file record starts with a little-endian u32 path length,
then that many UTF-8 path bytes, then the unchanged file contents. Paths are
logical names, not instructions to write files. Manifest references such as
`events/sched/sched_switch/format` identify these records. Metadata precedes
trace data and is included independently in each capture file.

Trace payloads preserve the page header and acquired data. Snapshot-read
captures use `skip_bytes = 0`, with `committed_bytes` bounding the valid raw
data. Snapshot collection keeps the existing container version and record kinds.
Older mmap captures remain supported; their committed/skip envelope fields are
authoritative because mmap can expose a growing reader page. The exporter scans the prefix to reconstruct timestamp controls but
only emits events after `skip_bytes`. Bytes beyond the committed frontier
are never interpreted as events. See [tracefs contract](tracefs.md).

The decoder preserves complete records before a torn tail. For a corrupt
stream record it scans for a later valid reset record, clears trace continuity,
and reports the gap. Damage to the startup metadata is not skipped. Any recovery
leaves the capture marked incomplete even if a later end record is present.
A complete end record in an undamaged capture indicates logical completion; only the live
service's successful final file and directory sync completions establish
that it observed durable completion. Reading a completed-looking file before
its final sync does not prove durability. A corrupt raw event within an
otherwise checksum-valid record is an export error, not silently discarded.

The format has no footer index or reliance on filesystem file length. Version
1 readers bound allocation and decompression. `retroft-validate` checks framing,
checksums, compressed payloads and completion; `retroft-export` additionally validates
the metadata schema and raw ftrace ABI while producing native Perfetto output.

The manifest's optional `kernel_release` contains the recording kernel's
release string (for example `6.1.0`). Perfetto export emits this in SystemInfo
before events so its scheduler-state decoder selects the matching Linux ABI.
Older captures without this field remain readable, with an export warning;
the exporter never substitutes the analysis machine's kernel version.

Named `syscalls:sys_enter_*` and `syscalls:sys_exit_*` records are exported both
as generic ftrace events (preserving named fields) and separate native Perfetto
`SysEnterFtraceEvent` / `SysExitFtraceEvent` messages. The native messages use
`__syscall_nr`, entry arguments in metadata offset order, and the signed exit
`ret`. Dynamic string fields stay in the generic representation. These companion
messages do not increase the export summary's source-event count.
The recorded `architecture` selects SystemInfo's machine name for x86-64,
AArch64, or ARM32; missing/unknown architectures produce only generic syscall
events and a warning. No architecture is inferred from the export host.
Compatibility ABIs are not distinguished. Pairing, unfinished slices, and syscall
names are handled by Perfetto's native importer; no missing endpoints are invented.

The Perfetto file also records the absolute source capture paths on the export
machine, in input order, as a JSON array in string metadata named
`retroft.source_capture_paths`. Perfetto's Chrome metadata importer exposes this
as `cr-retroft.source_capture_paths` in its `metadata` table. These paths describe
export provenance; they are not paths saved by the recorder or file identities.

Default Perfetto export removes disconnected leading history and emits per-CPU
`FtraceEventBundle.previous_bundle_end_timestamp` hints (Perfetto v47+).
The initial common-history boundary is the maximum first retained decoded event
timestamp across all declared CPUs. For each CPU, the exporter selects its last
kernel overrun whose first surviving event is at or before that boundary, and
omits the prefix preceding it. An overrun on an empty page is associated with the
next surviving event, including an mmap continuation. CPUs without qualifying
overruns retain their older events. The boundary is fixed: later overruns do not
recursively advance it. Missing history on any declared CPU disables cleanup.
Corruption/recovery losses are not treated as ordinary leading kernel overruns.
Original per-CPU loss accounting remains in the summary alongside cleanup counts.

Because the preceding event may have been overwritten, the exporter uses the
original first retained event timestamp as a conservative start boundary, rather than
claiming to recover that preceding timestamp. All boundary bundles precede
event bundles and use the same ftrace clock. Perfetto takes their maximum and
suppresses derived scheduling/thread-state interpretation before it. Cleanup
never moves a CPU's first event beyond that boundary, so the global hint remains
valid; kept early events remain in the file. This policy is applied across the whole export,
including joined captures, and is not restarted at file boundaries. A CPU with
no retained events supplies no hint and produces a warning: complete CPU
coverage cannot be established for it. These hints address unequal leading
history, not later overruns or gaps.

`retroft-export --raw` disables both leading-prefix cleanup and these native
boundary hints, preserving all decodable recorded events and loss markers for
inspection. It still validates framing, raw pages, and capture-chain continuity.
It does not manufacture missing events or make derived scheduling intervals
reliable across gaps. Neither mode modifies the original capture.

## Capture chains

Each recorder run generates an ISO 8601 basic UTC `session_id` with three random
words from a built-in 256-word dictionary, for example
`20261006T123456Z_monkey-waffle-dolly`. A separate random 128-bit `session_key`
is stored as 32 hex digits in the manifest, and compared by multi-capture export.
The session ID uses the daemon startup time and names a directory under `--output`.
Each file inside uses its own capture start time, the same word tuple, and its
capture index, for example:

```text
captures/20261007T201030Z_monkey-waffle-dolly/
  20261007T201045Z_monkey-waffle-dolly_1.retroft
  20261007T201112Z_monkey-waffle-dolly_2.retroft
```

Underscores separate fields; hyphens separate the three words.
All filename timestamps are UTC, with one-second precision. Capture indices
keep filenames distinct even within one second or if the wall clock moves back.
An existing session directory causes a fresh word tuple to be chosen.
Thus coincidentally matching readable names cannot establish chain identity.
File creation still refuses existing paths. Readers treat session IDs as opaque
strings; previous UUID-based and hex-suffixed IDs remain valid.
Its manifest includes:

```json
"capture_chain": {
  "schema_version": 1,
  "capture_index": "2",
  "previous_capture_index": "1"
}
```

Capture indices start at `"1"` and count files, independently of trigger
sequences (several triggers can extend one capture). The first predecessor is
`null`. A predecessor is assigned only after that capture's final file and
directory synchronization succeeds. End records repeat `capture_index`.
A restart creates a new session; indices never imply continuity across restarts.

Multi-capture export requires consecutive indices in the supplied order, matching
predecessors and session IDs, identical session metadata and event formats, and
complete, uncorrupted inputs with matching end indices. Old files without chain
metadata can still be exported individually. Any consecutive subset is valid;
it need not start with capture 1. Filenames are not evidence of continuity.

The exporter emits one Perfetto stream, preserving per-CPU stream continuity
(and legacy mmap frontiers),
timestamps, and event ordering across file boundaries. It emits initial task
identity once and combines clock snapshots, so Perfetto can maintain scheduler
and thread state across captures. LZ4 dictionaries still reset per input file.
Chain adjacency does not imply loss-free recording: kernel overrun indicators
remain in the merged output. Missing or corrupt files are rejected, rather than
bridging an unknown interval. Events never recorded cannot be reconstructed.

## Snapshot accounting

The recorder uses two preallocated ftrace banks and consume frozen data with
`snapshot_raw`. Each bank is drained before reuse. This changes collection, not
the raw ftrace layout or outer file framing. Earlier live-read and mmap captures
remain readable under the same compatibility policy.

Accounting records retain tracefs statistics for the live bank at sampling time.
Bank swaps and recycling can reset or change counters. Readers must not subtract
start/end counters to infer capture loss. Missed-event flags and any stored loss
counts in raw trace records are the authoritative evidence of detected kernel
loss; zero reported loss cannot prove that every desired event was collected.

When recording-side history trimming is enabled, end accounting contains an
optional `cpu_boundaries[].history_discard` object for each CPU:

- `margin_ns` and nullable `cutoff_ns`: decimal strings; a null cutoff means no
  confirmed leading loss supplied a common-window boundary.
- `pages` and `raw_bytes`: intentionally omitted whole pages and their total
  uncompressed allocation size, including headers and padding.
- `kernel_loss_pages`, `known_lost_events`, and `unknown_loss_pages`: loss flags
  found on omitted pages, the sum of explicitly stored loss counts, and pages
  whose loss count was unavailable. The known sum is not a complete loss count
  when any unknown counts are present.

Page counters are JSON integers; bytes, nanoseconds, and event counts are
decimal strings. An absent object means trimming was disabled or the recorder
predates this accounting. Intentional omission is separate from kernel loss;
these counters must not be converted into ftrace overrun flags. Raw trace
records retain loss markers on preserved pages. Neither the exporter nor its
`--raw` option can reconstruct intentionally omitted history. This optional
accounting extension does not change the container version or record kinds.
The manifest also stores `history_margin_ns` (decimal string, or null when
disabled) and `history_probe_bytes` (decimal string). The probe reserves two
sub-buffers per CPU to recognize a retained reader
fragment preceding an overwrite marker. Discarded fragments are included in
`history_discard` accounting. These fields preserve the policy
and cache allocation even if a torn capture lacks its final accounting. Missing
fields identify an older recorder, not proof of a particular trimming policy.

## Trigger source identity

Trigger JSON may include `source`, a client-provided UTF-8 label of at most
256 bytes. Absent or null means no label. The recorder also supplies integer
`peer_pid`, `peer_uid`, and `peer_gid` from the received Unix datagram's
`SCM_CREDENTIALS`, and an optional `peer_process_name` read from `/proc/PID/comm`
when the message is received. Missing identity fields mean unknown; readers accept their absence.
Credentials identify the sender of that message in the recorder's namespaces,
including when a socket is inherited or transferred. Process names are best
effort and may be unavailable after sender exit, stale, or affected by PID reuse.
Source labels and process names are not authenticated service identities. The exporter
shows the source in the trigger marker name and these fields in its annotations;
credential PID values do not associate the marker with a traced process.

## Clock snapshot records

Record kind 7 carries an uncompressed UTF-8 JSON object:

```json
{
  "schema_version": 1,
  "monotonic_ns": "123000000000",
  "boottime_ns": "183000000000",
  "realtime_ns": "1791320400000000000",
  "uncertainty_ns": "1200",
  "reason": "file_start"
}
```

All four time/uncertainty fields are canonical unsigned decimal strings in
nanoseconds. `monotonic_ns`, `boottime_ns`, and `realtime_ns` sample
CLOCK_MONOTONIC, CLOCK_BOOTTIME, and CLOCK_REALTIME respectively. The reason is
`file_start`, `before_resume`, or `resume`. A fresh `file_start` observation is
taken just before a capture file is created. A detected boottime-minus-monotonic
offset change retains the last observation as `before_resume` and the new
observation as `resume`.
The envelope timestamp equals `monotonic_ns`; CPU is `0xffffffff`, and the
committed/skip fields are zero. The sampling helper brackets CLOCK_REALTIME
with boottime reads and brackets those with monotonic reads. `uncertainty_ns`
is the larger elapsed bracket; samples repeatedly crossing suspend fail
rather than silently combine inconsistent clock epochs.

The daemon monitors clock relationships approximately once per second, and
also refreshes them immediately before file creation. Resume detection uses
an offset-change tolerance of 1 ms plus the relevant sampling uncertainties;
observations with more than 1 ms sampling uncertainty do not replace its
monitor baseline. It retains offset-change history and its latest usable
observation instead of retaining
every periodic sample. Each capture contains clock history as ordinary framed
records after its bootstrap metadata and before trace data; newly detected
changes are also written into an active capture. Capture files remain
continuous across suspend/resume, with no rollover or forced flush for a
clock-offset change.
Detected before/after pairs are retained since daemon startup without a
configured history cap; this small metadata allocation grows with detected
changes, not with every monitoring period. Changes detected after End seals
a capture are included in the next file. The file-start snapshot follows
bootstrap metadata in the initial write, before its starting accounting and
trigger marker; retained history follows before further trace/marker records.

The exporter prescans the valid file prefix for clock snapshots, sorts them
by monotonic time, and deduplicates identical observations. It bounds this
pass to 65,536 snapshots. It extends the earliest available correlation backward to
earlier history, then applies piecewise mappings from each subsequent
snapshot. Snapshot-aware exports preserve the primary monotonic scheduling
timeline and emit native Perfetto clock snapshots for relationships to
boottime and approximate UTC. Legacy files without these records retain their
previous boottime mapping.

Perfetto's ftrace clock enum lacks MONOTONIC. For snapshot-aware monotonic
captures, the exporter uses the enum's GLOBAL value only to select a supported
sequence-local clock and defines that clock as equal to MONOTONIC in native
clock snapshots. The kernel manifest and raw timestamps remain `mono`.
Original scheduling timestamps and intervals are preserved; GLOBAL here is
an encoding choice, not a request to use Linux's global clock.

These are sampled clock relationships, not exact suspend boundaries.
UTC/boottime correlation for events from the first resumed activity through
the post-resume sample uses the old observation. Their original monotonic
timestamps remain unchanged. Scheduling delays can enlarge that interval beyond a fraction
of a second; there is no hard bound. Multiple suspend cycles between monitor
observations, overwritten history, and lost events cannot be reconstructed
exactly. `uncertainty_ns` describes clock sampling uncertainty, not a bound
on the unobserved resume interval or UTC accuracy.

UTC correlation relies on the host's CLOCK_REALTIME. Periodic monitoring does
not record every NTP adjustment or realtime clock step; snapshots are stored
at file creation and detected resume changes. Old retained observations can
carry stale realtime correlation across NTP changes. UTC is therefore approximate.

## Compatibility policy

Before the first release, development files need not remain readable and no
compatibility branches are retained solely for them. The current container
version remains 1 while this contract is being finalized.

After release, a new exporter must read all previously released container
versions. Recorders emit the latest version only. Keep permanent input fixtures
for each released version, with validation and export tests; rewriting a fixture
to the latest format does not exercise backward compatibility.

The container version identifies the overall recording contract: framing,
record kinds, codecs, flag meanings, and required JSON/raw metadata semantics.
A breaking change bumps it, even when header geometry remains unchanged.
Compatible optional JSON fields may be added without a container bump if their
meaning and default when absent are documented. Readers ignore unknown optional
fields and apply those defaults. Required JSON schema changes need an explicit
supported schema/container contract. Unsupported versions, record kinds, codecs,
and flags are rejected, never silently reinterpreted or treated as optional.
