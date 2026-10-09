use anyhow::{ensure, Result};
use clap::{Parser, ValueEnum};
use retroft::{daemon, status::TriggerStatus, tracefs::TraceConfig, StatusEvent};
use std::{path::PathBuf, time::Duration};

mod sizes;

// The standalone executable owns process signals. The library never installs
// handlers; embedded applications use their own lifecycle and Shutdown handle.
static SHUTDOWN: std::sync::OnceLock<retroft::Shutdown> = std::sync::OnceLock::new();
extern "C" fn stop_signal(_: libc::c_int) {
    // Initialized before handlers are installed. Preserve interrupted code's
    // errno around the async-signal-safe wakeup write.
    unsafe {
        let saved = *libc::__errno_location();
        if let Some(shutdown) = SHUTDOWN.get() {
            shutdown.request();
        }
        *libc::__errno_location() = saved;
    }
}
fn install_signals() -> Result<&'static retroft::Shutdown> {
    let shutdown = retroft::Shutdown::new()?;
    SHUTDOWN
        .set(shutdown)
        .map_err(|_| anyhow::anyhow!("signal handlers already initialized"))?;
    // SAFETY: handler only requests shutdown; sigaction storage is initialized.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = stop_signal as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        for signal in [libc::SIGINT, libc::SIGTERM] {
            if libc::sigaction(signal, &sa, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
    }
    Ok(SHUTDOWN.get().unwrap())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Compression {
    Lz4,
    None,
}

#[derive(Parser)]
#[command(version, about = "Retroft: triggered ftrace collection for Linux")]
struct Cli {
    #[arg(long, default_value = "/sys/kernel/tracing")]
    tracefs: PathBuf,
    /// Named tracefs instance: a separate set of trace buffers and event settings.
    #[arg(long)]
    instance: Option<String>,
    /// Ownership file for safely reclaiming this tracefs instance after a crash.
    #[arg(long, requires = "instance")]
    instance_owner: Option<PathBuf>,
    #[arg(long, default_value = "/run/retroft.sock")]
    socket: PathBuf,
    /// Octal socket permissions. Default restricts clients to daemon owner.
    #[arg(long, default_value = "600")]
    socket_mode: String,
    /// Existing output directory; Retroft creates session directories inside it.
    #[arg(long, default_value = "captures")]
    output: PathBuf,
    /// Per-CPU size of each of two buffer banks; one value or a list in CPU order.
    #[arg(long, default_value = "4MiB", value_delimiter = ',', value_parser = sizes::parse_size, allow_hyphen_values = true)]
    buffer_per_cpu: Vec<usize>,
    /// Kernel sub-buffer size. Default prefers 64KiB, falling back to the kernel size.
    /// Explicit sizes are strict and must be a power-of-two multiple of system page size.
    #[arg(long, value_parser = sizes::parse_size, allow_hyphen_values = true)]
    subbuffer: Option<usize>,
    /// Staging memory budget, e.g. 8MiB; block count is automatic. MiB is binary, MB decimal.
    #[arg(long, default_value = "8MiB", value_parser = sizes::parse_size, allow_hyphen_values = true)]
    staging: usize,
    /// Stored bytes per synced batch, e.g. 8MiB. Whole records can exceed the threshold.
    #[arg(long, default_value = "8MiB", value_parser = sizes::parse_size, allow_hyphen_values = true)]
    sync_batch: usize,
    /// Keep this much extra history before the common CPU window; 0s is allowed.
    #[arg(long, default_value = "1s", value_parser = duration, allow_hyphen_values = true)]
    history_margin: Duration,
    /// Preserve all initial history instead of discarding pages before the common window.
    #[arg(long, conflicts_with = "history_margin")]
    keep_leading_history: bool,
    /// Report retained-history summaries while idle, e.g. 5s. Off by default.
    #[arg(long, value_parser = positive_duration, allow_hyphen_values = true)]
    idle_report: Option<Duration>,
    /// Trace compression; use none to minimize CPU work. Builds without LZ4 default to none.
    #[arg(long, value_enum, default_value = if cfg!(feature = "lz4") { "lz4" } else { "none" })]
    compression: Compression,
    /// Reset compression history after this much uncompressed trace data.
    #[arg(long, default_value = "4MiB", value_parser = sizes::parse_size, allow_hyphen_values = true)]
    compression_reset: usize,
    /// Replace default scheduler events; repeat --event SYSTEM:NAME.
    #[arg(long = "event")]
    events: Vec<String>,
    /// Record available named syscall entries and exits (native ABI only).
    #[arg(long)]
    syscalls: bool,
    /// Record available hard-IRQ handlers and soft-IRQ raise/entry/exit events.
    #[arg(long)]
    irqs: bool,
    /// Add optional events if available; repeat this option.
    #[arg(long = "optional-event")]
    optional_events: Vec<String>,
}
fn duration(value: &str) -> std::result::Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    u64::try_from(duration.as_nanos()).map_err(|_| "duration exceeds nanosecond range")?;
    Ok(duration)
}
fn positive_duration(value: &str) -> std::result::Result<Duration, String> {
    let duration = duration(value)?;
    if duration.is_zero() {
        return Err("duration must be at least one nanosecond".into());
    }
    Ok(duration)
}
fn kernel_kib(bytes: usize, argument: &str) -> Result<usize> {
    ensure!(
        bytes > 0 && bytes.is_multiple_of(1024),
        "{argument} must resolve to an exact multiple of 1024 bytes (1KiB)"
    );
    Ok(bytes / 1024)
}
fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let Cli {
        tracefs,
        instance,
        instance_owner,
        socket,
        socket_mode,
        output,
        buffer_per_cpu,
        subbuffer,
        staging,
        sync_batch,
        history_margin,
        keep_leading_history,
        idle_report,
        compression,
        compression_reset,
        events,
        syscalls,
        irqs,
        optional_events,
    } = Cli::parse();
    let buffer_kb_per_cpu = buffer_per_cpu
        .into_iter()
        .map(|bytes| kernel_kib(bytes, "--buffer-per-cpu"))
        .collect::<Result<Vec<_>>>()?;
    let subbuffer_kb = subbuffer
        .map(|bytes| kernel_kib(bytes, "--subbuffer"))
        .transpose()?;
    let mut trace = TraceConfig {
        tracefs,
        owner_file: instance_owner,
        buffer_kb_per_cpu,
        subbuffer_kb,
        syscalls,
        irqs,
        ..TraceConfig::default()
    };
    if let Some(name) = instance {
        trace.instance = name;
    }
    if !events.is_empty() {
        trace.events = events;
    }
    // Scheduler-only by default: high-volume optional classes are opt-in.
    trace.optional_events = optional_events;
    let source =
        retroft::trigger::UnixSocket::open(&socket, u32::from_str_radix(&socket_mode, 8)?)?;
    let mut config = daemon::Config::new(output);
    config.trace = trace;
    config.staging_bytes = staging;
    config.sync_batch_bytes = sync_batch as u64;
    config.history_margin = (!keep_leading_history).then_some(history_margin);
    config.idle_report = idle_report;
    config.compression = match compression {
        Compression::Lz4 => Some(compression_reset),
        Compression::None => None,
    };
    let shutdown = install_signals()?;
    let mut notify_error = None;
    let result = daemon::run(config, shutdown, source, |mut event| {
        if let retroft::StatusEvent::Ready { summary, .. } = &mut event {
            use std::fmt::Write;
            let _ = write!(summary, " socket={}", socket.display());
            if let Err(error) = retroft::activation::notify_ready() {
                notify_error = Some(error);
                shutdown.request();
            }
        }
        report(event);
    });
    result?;
    if let Some(error) = notify_error {
        return Err(error);
    }
    Ok(())
}

fn trigger_text(trigger: &TriggerStatus) -> String {
    // Debug formatting escapes newlines/control characters supplied by clients.
    format!(
        "trigger={} source={:?} pid={} description={:?}",
        trigger.sequence,
        trigger.source.as_deref().unwrap_or("unknown"),
        trigger.pid.unwrap_or(0),
        trigger.description
    )
}

fn report(event: StatusEvent) {
    match event {
        StatusEvent::Ready { summary, .. } => eprintln!("{summary}"),
        StatusEvent::CaptureStarted { path, trigger, .. } => eprintln!("capture started: {} {} post={:.3}s", path.display(), trigger_text(&trigger), trigger.post_remaining_ns as f64 / 1e9),
        StatusEvent::TriggerAdded { capture_index, trigger, extension_ns, .. } => {
            let action = if extension_ns > 0 { "extended" } else { "trigger added" };
            eprintln!("capture {action}: #{capture_index} {} extension={:.3}s post_remaining={:.3}s", trigger_text(&trigger), extension_ns as f64 / 1e9, trigger.post_remaining_ns as f64 / 1e9);
        }
        StatusEvent::CaptureCompleted { bytes, elapsed_ns, triggers, snapshot_banks, .. } => eprintln!("capture complete: {:.2} MiB, {:.3}s elapsed, triggers={}, snapshot_banks={}; synced", bytes as f64 / (1024.0 * 1024.0), elapsed_ns as f64 / 1e9, triggers, snapshot_banks),
        StatusEvent::Overrun { cpu, .. } => eprintln!("warning: overrun during capture: cpu={}; recovered data and loss markers retained; further warnings suppressed for this CPU in this capture", cpu),
        StatusEvent::Idle { cpus } => {
            if let Err(error) = retroft::tracefs::write_idle_status(&cpus, &mut std::io::stderr().lock()) {
                eprintln!("idle report unavailable: {error:#}");
            }
        }
        StatusEvent::Diagnostic { message } => eprintln!("{message}"),
        _ => {} // Future status variants need not break older integrations.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_trimming_defaults_and_explicit_override() {
        let defaults = Cli::try_parse_from(["recorder"]).unwrap();
        assert_eq!(defaults.history_margin, Duration::from_secs(1));
        assert!(!defaults.keep_leading_history);
        assert!(
            Cli::try_parse_from(["recorder", "--keep-leading-history"])
                .unwrap()
                .keep_leading_history
        );
        let exact = Cli::try_parse_from(["recorder", "--history-margin", "0s"]).unwrap();
        assert_eq!(exact.history_margin, Duration::ZERO);
        assert!(Cli::try_parse_from([
            "recorder",
            "--keep-leading-history",
            "--history-margin",
            "2s"
        ])
        .is_err());
        for invalid in ["-1s", "NaN", "1", "18446744073709551616ns"] {
            assert!(Cli::try_parse_from(["recorder", "--history-margin", invalid]).is_err());
        }
    }

    #[test]
    fn compression_reset_uses_explicit_byte_units() {
        let defaults = Cli::try_parse_from(["recorder"]).unwrap();
        assert_eq!(
            defaults.compression,
            if cfg!(feature = "lz4") {
                Compression::Lz4
            } else {
                Compression::None
            }
        );
        assert_eq!(defaults.compression_reset, 4 * 1024 * 1024);
        let cli = Cli::try_parse_from([
            "recorder",
            "--compression",
            "lz4",
            "--compression-reset",
            "8MiB",
        ])
        .unwrap();
        assert_eq!(cli.compression, Compression::Lz4);
        assert_eq!(
            Cli::try_parse_from(["recorder", "--compression", "none"])
                .unwrap()
                .compression,
            Compression::None
        );
        assert!(Cli::try_parse_from(["recorder", "--compression", "invalid"]).is_err());
        assert_eq!(cli.compression_reset, 8 * 1024 * 1024);
        for value in ["0B", "4096", "-1MiB"] {
            assert!(Cli::try_parse_from(["recorder", "--compression-reset", value]).is_err());
        }
    }

    #[test]
    fn duration_defaults_and_units() {
        let defaults = Cli::try_parse_from(["recorder"]).unwrap();
        assert!(defaults.idle_report.is_none());
        assert_eq!(
            Cli::try_parse_from(["recorder", "--idle-report", "5s"])
                .unwrap()
                .idle_report,
            Some(Duration::from_secs(5))
        );
        for (value, expected) in [
            ("5s", Duration::from_secs(5)),
            ("100ms", Duration::from_millis(100)),
            ("0.5s", Duration::from_millis(500)),
            ("500000000ns", Duration::from_millis(500)),
            ("500ns", Duration::from_nanos(500)),
            ("1s 250ms", Duration::from_millis(1250)),
        ] {
            let cli = Cli::try_parse_from(["recorder", "--idle-report", value]).unwrap();
            assert_eq!(cli.idle_report, Some(expected));
        }
    }

    #[test]
    fn invalid_durations_fail_before_backend_setup() {
        for flag in ["--idle-report"] {
            for value in [
                "NaN",
                "inf",
                "-1s",
                "0s",
                "0ns",
                "0.1ns",
                "1e20s",
                "18446744073709551616ns",
                "1",
                "invalid",
            ] {
                assert!(
                    Cli::try_parse_from(["recorder", flag, value]).is_err(),
                    "accepted {flag} {value}"
                );
            }
        }
        for flag in [
            "--sync-interval",
            "--sync-ms",
            "--sync-secs",
            "--sync-ns",
            "--settle-time-ms",
            "--settle-time-secs",
            "--settle-time-ns",
        ] {
            assert!(Cli::try_parse_from(["recorder", flag, "1"]).is_err());
        }
    }

    #[test]
    fn memory_units_and_defaults() {
        let defaults = Cli::try_parse_from(["recorder"]).unwrap();
        assert_eq!(defaults.staging, 8 * 1024 * 1024);
        assert_eq!(defaults.sync_batch, 8 * 1024 * 1024);
        assert_eq!(defaults.buffer_per_cpu, [4 * 1024 * 1024]);
        assert_eq!(defaults.subbuffer, None);
        assert_eq!(
            Cli::try_parse_from(["recorder", "--sync-batch", "16MiB"])
                .unwrap()
                .sync_batch,
            16 * 1024 * 1024
        );
        for value in ["0B", "4096", "100ms", "-1MiB"] {
            assert!(Cli::try_parse_from(["recorder", "--sync-batch", value]).is_err());
        }
        let cli = Cli::try_parse_from([
            "recorder",
            "--staging",
            "16MB",
            "--buffer-per-cpu",
            "1.5MiB",
            "--subbuffer",
            "64KiB",
        ])
        .unwrap();
        assert_eq!(cli.staging, 16_000_000);
        assert_eq!(
            kernel_kib(cli.buffer_per_cpu[0], "--buffer-per-cpu").unwrap(),
            1536
        );
        assert_eq!(
            kernel_kib(cli.subbuffer.unwrap(), "--subbuffer").unwrap(),
            64
        );
        for flag in [
            "--staging-kib",
            "--staging-mib",
            "--staging-blocks",
            "--buffer-kb-per-cpu",
            "--subbuffer-kb",
        ] {
            assert!(Cli::try_parse_from(["recorder", flag, "1"]).is_err());
        }
    }

    #[test]
    fn per_cpu_lists_parse_but_kernel_units_must_be_exact() {
        let cli =
            Cli::try_parse_from(["recorder", "--buffer-per-cpu", "4MiB,2MiB,1.5MiB"]).unwrap();
        assert_eq!(
            cli.buffer_per_cpu
                .iter()
                .map(|bytes| kernel_kib(*bytes, "--buffer-per-cpu").unwrap())
                .collect::<Vec<_>>(),
            [4096, 2048, 1536]
        );
        for value in ["", ",4MiB", "4MiB,", "4MiB,,2MiB"] {
            assert!(
                Cli::try_parse_from(["recorder", "--buffer-per-cpu", value]).is_err(),
                "accepted {value}"
            );
        }
        for bytes in [0, 1, 1000, 1_000_000, 1025] {
            assert!(kernel_kib(bytes, "--buffer-per-cpu").is_err());
            assert!(kernel_kib(bytes, "--subbuffer").is_err());
        }
        assert_eq!(
            kernel_kib(sizes::parse_size("1.024kB").unwrap(), "--buffer-per-cpu").unwrap(),
            1
        );
    }
}
