# Subbuffer compression investigation

Measurements from a single x86-64 test environment, pinned to one logical CPU,
with a powersave governor and boost enabled. Hardware-identifying details are
omitted; absolute timings are illustrative, not reproducible performance targets. LZ4 1.10.0 and
Zstandard 1.5.7. These are synthetic trace-like payloads, not captured kernel
traces or a target-system latency test. This benchmark predates the switch to linked LZ4 in production.

## Results

Each table cell is average stored bytes / compression microseconds per block.
Time is the median of seven trial averages, not worst-case call latency.

| Payload | LZ4 | Zstd level 1 | Zstd level 1 + 8 KiB dictionary |
| --- | ---: | ---: | ---: |
| Scheduler, 4 KiB | 1743 / 4.08 | 1045 / 12.31 | 950 / 8.33 |
| Scheduler, 16 KiB | 6275 / 16.17 | 3858 / 36.72 | 3723 / 43.93 |
| Scheduler, 64 KiB | 22062 / 58.89 | 14819 / 120.69 | 14284 / 143.18 |
| Mixed, 4 KiB | 3117 / 6.60 | 2062 / 15.24 | 1969 / 16.52 |
| Mixed, 64 KiB | 45877 / 109.66 | 32326 / 175.21 | 30852 / 242.85 |
| Scheduler, quarter-full 4 KiB | 476 / 1.18 | 313 / 5.80 | 246 / 2.30 |

For full scheduler blocks at 4–64 KiB, LZ4 was about 2–3 times faster than
Zstd level 1, while Zstd output was 33–40% smaller. On mixed blocks in that
range, LZ4 was about 1.6–2.3 times faster and Zstd output 30–34% smaller.
LZ4's externally supplied state was 16,416 bytes; the conservative Zstd
level-1 static workspace was 582,680 bytes. With its prepared dictionary,
the Zstd case used 736,856 bytes. These exclude input/output buffers and
are not minimum possible Zstd memory requirements.

The dictionary reduced 4 KiB scheduler output by another 9%, and quarter-full
4 KiB output by 21%, relative to plain Zstd level 1. It also improved speed
in those cases, but was slower on most larger blocks. Mixed 4 KiB output
improved only 4.5%. This supports investigating dictionaries for small,
predictable records, but does not establish a broadly useful premade dictionary.
The dictionary was trained separately for every workload and size, on different
random seeds from the test samples: an intentionally well-matched distribution.
Its storage overhead is excluded, so short captures would benefit less.

All 60 results, including Zstd level 3, level -3, and 256 KiB blocks, are in
[compression.csv](compression.csv). LZ4 is promising when CPU and workspace
matter most; Zstd is promising when disk bandwidth and storage dominate.
Neither these averages nor the largest trial average establish a jitter bound.
The recorder uses linked LZ4 by default; these independent-block measurements
do not establish its performance on a recording target.

## Method

[compression.c](compression.c) generates 64 distinct inputs per workload/size:
fixed-layout scheduler-like events with changing task IDs and timestamps;
variable-layout events with integers and address-like values; and scheduler
pages with zero-filled tails. These approximate byte patterns rather than
constituting a validated ftrace corpus. Every compressed block is decompressed
and checked before timing. Each of seven trials runs for at least 30 ms.
Contexts are reused, but blocks have no history from preceding blocks.
Workspace setup, dictionary training, and decompression are outside timing.
Contexts are touched during setup; codec order is fixed and frequency scaling
is uncontrolled, so this is a quick comparison rather than a controlled study.

Stored sizes use the smaller of compressed and original length, matching raw
fallback. Container overhead, checksums, disk writes, and dictionary storage
are excluded. MB/s uses decimal megabytes. CSV minimum/maximum times are
minimum/maximum *trial averages*, not individual-call extrema.

This run used the static Zstd archive from the earlier Zstd-based recorder.
The recorder now uses LZ4 and no longer builds that archive. To reproduce,
build Zstd 1.5.7 separately (including dictionary training), then use its
matching headers and static library with the system LZ4 development files:

```bash
# Set this to a local Zstd 1.5.7 source tree built with its static library.
zstd_root=/path/to/zstd-1.5.7
cc -O3 -Wall -Wextra -Werror \
  -I "$zstd_root/lib" benchmarks/compression.c \
  "$zstd_root/lib/libzstd.a" -llz4 -o /tmp/retroft-compression-bench
/tmp/retroft-compression-bench > benchmarks/compression.csv
```

The recorder's static archive supplies compression routines; the system Zstd
library supplies dictionary training omitted from that archive. Both were
version 1.5.7 for this run. Do not compile with `NDEBUG`: assertions perform
the benchmark's setup and correctness checks.

## Streaming alternative

Zstd's `ZSTD_e_flush` emits buffered input while retaining compression history;
`ZSTD_e_end` closes an independent frame. This could amortize small-subbuffer
overheads without a trained dictionary. Compressor flush must precede the
durability sync, and extraction must accept a decodable prefix of an unfinished
frame after a crash. Periodic independent frames can bound seek and corruption
dependencies. Streaming was not measured in this independent-block comparison.
See the [Zstd API manual](https://facebook.github.io/zstd/doc/api_manual_v1.5.7.html).
