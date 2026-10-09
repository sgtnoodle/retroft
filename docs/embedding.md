# Embedding Retroft

An integration can live in its own executable crate and depend on the `retroft`
library. No custom capture-loop patch, worker thread, async runtime, message-bus
library, or exporter dependency is required. Start with `daemon::Config::new`
and override only the settings you need. Both `config.trace.syscalls = true` and
`config.trace.irqs = true` work identically for the CLI and embedded callers.
The output directory must already exist.

## Using the crates in another repository

The easiest options preserve Retroft's workspace: use a Git dependency pinned to
an immutable commit, or keep a complete checkout as a local dependency. Neither
requires adding Retroft's crates to your application's workspace members.

For a Git dependency, replace the placeholders with your repository URL and commit:

```toml
[dependencies]
retroft = { git = "<Retroft repository URL>", rev = "<full commit hash>" }
```

Cargo finds the named package inside the repository and resolves its sibling
`retroft-format` dependency. Use the same URL and revision for any additional
Retroft packages. Depending on `retroft` does not build the exporter or install
the CLI binaries. Use `retroft-trigger` instead if your process only sends triggers.

For a checkout, submodule, or source copy under `third_party/retroft`, keep the
root `Cargo.toml` and the `crates/` tree together. In the consuming workspace root:

```toml
[workspace]
members = ["app"]
exclude = ["third_party/retroft"]

[workspace.dependencies]
retroft = { path = "third_party/retroft/crates/retroft" }
```

Then use `retroft.workspace = true` under `[dependencies]` in `app/Cargo.toml`.
Merge these entries into your existing tables. Retroft retains its own workspace
for inherited manifest settings; your application's lockfile and build profiles
control the consuming build. The exporter is built only when selected.

### Copying individual crates into your workspace

This also works, but copying the directories alone is not sufficient:

1. Copy `retroft` and `retroft-format` together for recorder integration.
   `retroft-trigger` is independent; `retroft-export` also needs `retroft-format`.
   Keep each crate's `LICENSE` and retain the applicable third-party notices.
2. Add the copied packages to your workspace members.
3. Merge the required entries from Retroft's root
   [`[workspace.dependencies]`](../Cargo.toml) into yours, including their features,
   or replace each `workspace = true` dependency with an explicit declaration.
4. `retroft` and `retroft-format` inherit `version` and `edition` from
   `[workspace.package]`. Supply those fields in your workspace or replace the
   inherited fields in the copied manifests with their current explicit values.
5. Keep `retroft-format` beside `retroft`/`retroft-export`, or update their
   `path = "../retroft-format"` dependencies.

The parent directory need not be named `crates`; only relative paths and workspace
membership matter. Keeping the original checkout intact usually makes upstream
updates easier than maintaining copied manifests.

### Cargo vendoring

For a Git dependency, ordinary `cargo vendor` works as it does for other external
crates. Run it from your consuming workspace, then merge the source-replacement
configuration it prints into `.cargo/config.toml`. Commit the vendor directory,
configuration, and application lockfile; verify with `cargo build --locked --offline`.
Preserve your existing Cargo settings when adding source replacement.

`cargo vendor` collects registry and Git dependencies, not local path dependencies.
If you use the checkout/path approach, retain that source tree yourself; Cargo can
still vendor its external dependencies. See Cargo's documentation for
[vendoring](https://doc.rust-lang.org/cargo/commands/cargo-vendor.html) and
[workspace inheritance](https://doc.rust-lang.org/cargo/reference/workspaces.html).

## One thread or a dedicated recorder thread

`daemon::Recorder::new(config, shutdown, source, callback)` initializes tracing and storage
and emits `StatusEvent::Ready`. The caller creates the trigger source and transfers
ownership to the recorder, which keeps its descriptors open while registered.

Call `recorder.step(callback)` for one turn:

- `Step::Progress`: more work may be ready. Service other application work, then
  step again without sleeping.
- `Step::Pending`: wait for the recorder's readiness descriptor.
- `Step::Finished`: shutdown has completed; accepted capture data is synced and
  the tracing instance has been torn down.

`Recorder` implements `AsFd`. Register its one descriptor with your application's
level-triggered poll or epoll loop. Retroft manages its own epoll set and monotonic
timerfd, including trigger admission, storage completions, and shutdown wakeups.
The descriptor stays readable while immediate work remains, so the application
can yield between steps. Do not read it or call `epoll_wait` on the child epoll;
`step` owns its notifications. Remove your registration when finished or failed.
No timeout calculation or changing descriptor list is exposed to the caller.
The standalone runner uses this same readiness mechanism. Timer and registration
updates happen when deadlines or interests change, not for every sub-buffer.

Each step processes at most one raw sub-buffer, up to 16 incoming requests, and
storage completion handling. It does not wait for input or storage completion.
This is **cooperative scheduling, not a latency bound**: snapshot
swaps, `membarrier`, file creation, directory syncing, and other kernel calls can
block. Initialization and destruction can also block. Applications that cannot
allow those pauses should run the recorder on a dedicated thread or process.

The core turn follows a fixed order: reap completed storage operations, drain one
page, accept triggers once the frozen bank is drained, then finish the capture or
swap another bank. Closing appends final accounting and End together, waits for
writes, and waits for the final sync before reporting completion. Requests arriving
after End wait for the next capture. The same ordering applies in the CLI and an
embedded loop.

`daemon::run(config, &shutdown, source, callback)` is the blocking convenience
loop using this same API. Call it inside the recorder thread and keep a cloned
`Shutdown` handle outside it.

For example, given a configured `config` and a trigger `source`:

```rust
use retroft::{daemon, Shutdown};

let shutdown = Shutdown::new()?;
let worker_shutdown = shutdown.clone();
let worker = std::thread::spawn(move || {
    daemon::run(config, &worker_shutdown, source, |event| {
        // Forward status to your application without blocking this thread.
    })
});

// Run the rest of your application. When it is time to stop:
shutdown.request();
worker.join().expect("recorder thread panicked")?;
```

The moved source and callback captures must be `Send + 'static` for
`std::thread::spawn`; alternatively, construct them inside the worker. Recorder
initialization happens in the worker, and `StatusEvent::Ready` signals that it
succeeded. `request()` wakes the worker; joining waits for accepted capture data
to finish syncing and tracing teardown to complete. The returned recorder error
propagates through `join()` separately from a thread panic. Keep stepping or let
`run()` return normally for graceful shutdown; dropping a `JoinHandle` only
detaches the thread.

## Trigger sources

The recorder consumes the `trigger::TriggerSource` trait: a borrowed pollable fd
and a nonblocking `receive()` method. Each call returns one typed request, an
empty indication, or an indication that one malformed transport message was
consumed. Readiness must remain asserted while requests are pending. Source errors
are fatal to the recorder; adapters should distinguish bad messages from failures.

`TriggerRequest::new(source, description, endpoint)` uses a required source label,
an optional description (use an empty string), and an optional absolute
CLOCK_MONOTONIC endpoint in nanoseconds. `None` means admission time. The recorder
validates size and endpoint limits and assigns the sequence number and timestamp
on admission, regardless of transport. Invalid requests are discarded without
consuming a sequence number, matching Unix-socket behavior. Validate earlier in
your adapter if the remote caller needs an error response.

Two adapters are included:

- `trigger::UnixSocket::open(path, mode)` binds the standard trigger protocol or
  accepts a systemd activation descriptor. The CLI constructs this adapter and passes it to the recorder. Its lifetime
  controls socket cleanup; dropping the recorder drops the adapter. `bind` explicitly
  bypasses activation. Kernel-supplied sender credentials are retained.
- `trigger::channel(capacity)` supplies a bounded typed queue and a pollable receiver.
  The cloneable sender's `try_send` reports `Full` or `Disconnected` without waiting.
  This is optional: use it across threads, or when your single-threaded bus loop
  must continue handling unrelated messages while trigger admission is paused.
  Typed requests do not claim a kernel-authenticated sender PID.

Enqueueing is not acceptance or durability. `CaptureStarted` and `TriggerAdded`
report admission. Queue capacity counts requests, not bytes; keep payloads within
the documented source/description bounds before enqueueing. The channel is a
convenience for integrations, not a hard-real-time primitive; its standard-library
synchronization is not wait-free. Critical callers should use the documented
`retroft-trigger` client and Unix datagram protocol.

## Status and lifecycle

Status goes directly to a synchronous callback. No status queue is imposed.
Events own their fields, so the callback can move them into an application-owned
bounded queue using `try_send`. Decide explicitly what happens on overflow:
drop/count telemetry, coalesce idle reports, or request recorder shutdown if
losing lifecycle notifications is unacceptable. Never block the recorder waiting
for a slow bus subscriber. Callback panics unwind the recorder; do not use panic
as an error-delivery mechanism.

Typed events include readiness, capture start, additional/extended triggers,
completion, overruns, and per-CPU idle statistics. Supplementary diagnostics and
the readiness summary are human-readable text, not fields to parse. The library
does not print them. The events support `Serialize`, but their JSON is not a stable protocol. Define
and version your application's bus schema, including how it encodes 64-bit timestamps.

`CaptureCompleted` means the file's final sync and its creation-time directory
sync succeeded. Errors, including setup and teardown failures, are returned by
`new`, `step`, or `run`; the caller reports failure to its supervisor. After an
error, drop the recorder rather than calling step again. Cleanup after an earlier
error is best-effort and does not replace the original error.

`Shutdown::request()` wakes a waiting recorder, stops admission, shortens a future
capture endpoint to the shutdown time, and allows accepted data to finish. Keep
stepping until `Finished`. Dropping the last sender or a shutdown handle does not
request shutdown. Dropping an active recorder aborts capture and attempts cleanup. Requests still queued at shutdown are not accepted or replayed.
The recorder owns the source; drop the recorder when finished to release it.

The CLI installs SIGINT/SIGTERM handlers and sends systemd `READY=1` upon readiness.
The library installs no signal handlers and sends no systemd notifications itself;
`activation::notify_ready()` remains available to wrappers that want it.

## Single-threaded example

[`embedded_loop.rs`](../crates/retroft/examples/embedded_loop.rs) combines the recorder
and a small demonstration bus in one `poll` loop. Status delivery uses a single
nonblocking datagram send, counting dropped messages; trigger delivery uses a
16-request queue. There are no worker threads. The demonstration bus accepts:

```json
{"command":"trigger","source":"controller","description":"deadline missed","end_timestamp_ns":null}
{"command":"shutdown"}
```

Build with `cargo build -p retroft --example embedded_loop`. On a suitable
recording target, invoke `target/debug/examples/embedded_loop OUTPUT_DIR
COMMAND_SOCKET STATUS_SOCKET`. Provision the output directory, bind a status
receiver, and choose a private command-socket directory beforehand. The example
refuses an existing command socket path and leaves pathname cleanup to the caller.
Its command protocol is illustrative and does not replace the standard Retroft
trigger protocol. Adapt it to your bus's authentication and lifecycle rules.

## Keeping custom integrations easy to update

Keep bus dependencies, policy, and application code in a separate crate. Depend
on a pinned Retroft release or Git revision, and exercise that wrapper in your CI.
The integration API consists of `daemon::{Config, Recorder, Step, run}`,
`tracefs::TraceConfig`, `Shutdown`, and the `status` and `trigger` types.
Other publicly visible low-level modules are implementation details, not an
extension contract. Avoid patches to storage, compression, or the drain algorithm.

This project is still unreleased; the interface may change before its first
release. Once released, incompatible integration changes require a version change
that permits them under Cargo's compatibility rules and accompanying migration
notes. Use constructors and defaults rather than copying complete configuration
literals; match extensible status events with a fallback arm and `..` fields.
No dynamic plugin ABI or binary compatibility is promised.
