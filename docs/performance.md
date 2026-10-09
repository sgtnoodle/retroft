# Recorder performance assessment: 2026-10-07

This assessment uses two consecutive live x86-64 test recordings, referred to
as capture 1 and capture 2. The original captures are not distributed with the
repository. Machine-identifying details and private paths are omitted.
The recorder used 32 logical CPUs,
64 KiB sub-buffers, LZ4 compression, an 8 MiB stored-byte sync batch,
and five-second post-trigger requests. They used the previous 10 ms occupancy
check interval. The recorder now uses 100 ms; the swap threshold remains 50% of
any CPU's configured capacity. These measurements do not validate the 100 ms
interval under bursts.

Both captures have valid completion records. Consecutive export and native
Perfetto Trace Processor import reported no loss or import errors. There were
541 and 740 stored trace sub-buffers respectively. The files are approximately
7.1 and 9.0 MiB: the first stays below the periodic sync threshold, while the
second crosses it. Final accounting reports zero and two staging-exhaustion
observations respectively. An observation is not a duration or a lost-event count.

## First frozen-bank drain

Times below come from Retroft's own named syscall and scheduler events in the
joined export. The measured drain window starts at the return of the first
`membarrier` and ends at entry to the first long post-trigger `ppoll`. It therefore
includes the first occupancy check and a small amount of loop bookkeeping;
the `first_drain` log ends slightly earlier, at frozen-bank consumption.

| Measurement | Capture 1 | Capture 2 |
| --- | ---: | ---: |
| Initial snapshot `write` duration | 235.619 ms | about 0.012 ms |
| Following `membarrier` duration | 5.941 ms | 8.379 ms |
| First drain window | 25.562 ms | 35.880 ms |
| Recorder scheduled CPU time within drain window | 25.504 ms | 35.872 ms |
| Raw sub-buffers read in first drain | 334 | 495 |
| Padded raw data read | 20.875 MiB | 30.938 MiB |
| Raw-read syscall time | 2.148 ms | 3.378 ms |
| `io_uring_enter` calls in drain window | 334 | 495 |
| `io_uring_enter` syscall time in drain window | 0.457 ms | 0.741 ms |

All observed `io_uring_enter` submissions had `to_submit=1` and
`min_complete=0`; they were submissions, not explicit waits for a completion.
The drain was almost continuously scheduled. This does not measure CPU time
spent by asynchronous kernel workers or attribute the remaining recorder time
specifically to compression: that would require profiling.

The slow first snapshot write is distinct from the explicit writer barrier.
The exact kernel-side cause of its first-use latency is not established by
these syscall traces. Later snapshot writes were much shorter.

## Why reads and submissions appear in bursts

At the start of each drain, the history probe first reads two event-bearing
sub-buffers per CPU. These captures show exactly 64 initial raw reads before
trace-page write submissions begin. The recorder must inspect these pages
before choosing a common history cutoff and deciding whether to discard an
old retained reader fragment.

It then replays the cached pages, compressing and submitting them. That creates
a run of `io_uring_enter` calls without intervening raw reads. Some EOF checks
interrupt this run. Once the cache is exhausted, ordinary raw reads and write
submissions alternate. This particular initial burst is not a backlog released
by a completed storage sync.

The shape is expected: inspecting the prefix prevents writing data that must
be discarded. Interleaved submission afterward starts storage work promptly
while subsequent records are being read and compressed. The traces do not
prove that this is the optimal submission granularity.

## Is batching worthwhile now?

Submission syscall time is about 1.8% and 2.1% of these first drain windows.
Batching could remove some of it, but those numbers do not support making it
the next optimization priority. Batching the cached prefix would also require
deferring submissions across compression iterations; batching only a backlog
already present inside `Storage::pump()` would not combine most of this pattern.

Keep immediate submissions for now. Consider a small bounded submission batch
if another platform shows a larger contribution, and compare CPU time,
first-drain latency, staging pressure, and event loss before adopting it.
Do not delay writes solely to accumulate a larger batch.

Polling is a larger source of avoidable syscall traffic in these recordings:
each five-second request contains roughly 15,000--16,000 statistics reads and
a similar number of seeks, mostly from checking all 32 CPUs every 10 ms.
Changing the interval to 100 ms should reduce that periodic work by about tenfold,
though it does not reduce each read's cost and leaves a longer burst window.

## Reproduction and limits

To apply the same analysis to your own recordings, export two consecutive files
in capture order (replace the placeholder paths):

```sh
./target/release/retroft-export \
  /path/to/first.retroft \
  /path/to/second.retroft \
  -o /tmp/combined.pftrace
```

In Trace Processor, locate the recorder through `thread.name = 'retroft'`,
join `slice.track_id` to `thread_track.id`, and inspect syscall slices and
`EXTRACT_ARG(arg_set_id, 'args[0]')` for the first argument. Sum `sched` intervals clipped to the
same drain windows for scheduled CPU time. Determine these windows from the
first barrier return and following long poll in each recording.

The second file preserves activity after the first capture, so joining helps
observe the first file's finalization. The final bank's own draining/finalization
in capture 2 is outside its frozen trace history; its runtime is not measured
here. Final accounting still records its staging-exhaustion observations.

Tracing the recorder's syscalls itself adds workload. These are two observations,
not a benchmark distribution, a bound on critical-task jitter, proof of
power-loss durability, or validation of ARM/PREEMPT_RT behavior.
