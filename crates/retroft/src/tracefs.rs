//! Owned tracefs instance and opaque per-CPU raw sub-buffer acquisition.
//!
//! Snapshot swaps freeze a whole bank while tracing continues in its spare.
use crate::clock::clock_ns;
use crate::status::CpuIdleStatus;
use anyhow::{bail, ensure, Context, Result};
use serde::{ser::SerializeSeq, Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::{ffi::OsStrExt, fs::OpenOptionsExt};
use std::path::{Path, PathBuf};

// Combined event formats and header descriptions retained at startup.
const MAX_METADATA: usize = 8 * 1024 * 1024;
// Cap the best-effort /proc snapshot independently of event metadata.
const MAX_TASKS: usize = 65_536;
// Reused scratch for one CPU’s tracefs statistics file.
const STATS_CAPACITY: usize = 16 * 1024;
// Linux's commit length occupies the low 30 bits. Its signed (1 << 31)
// loss flag can sign-extend into the upper 32 bits of a 64-bit local_t.
const COMMIT_LENGTH_MASK: u64 = (1 << 30) - 1;

#[derive(Debug, Clone)]
pub struct TraceConfig {
    pub tracefs: PathBuf,
    pub instance: String,
    /// Persistent reservation and exclusive lock enabling owned crash recovery.
    pub owner_file: Option<PathBuf>,
    /// One KiB size for all CPUs, or one per online CPU in ascending ID order.
    pub buffer_kb_per_cpu: Vec<usize>,
    /// Explicit size is strict; None prefers 64 KiB with a kernel-size fallback.
    pub subbuffer_kb: Option<usize>,
    /// Discover named syscall entry/exit events and retain their format metadata.
    pub syscalls: bool,
    /// Enable available hard-IRQ and soft-IRQ events.
    pub irqs: bool,
    pub events: Vec<String>,
    pub optional_events: Vec<String>,
    pub trace_clock: String,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            tracefs: PathBuf::from("/sys/kernel/tracing"),
            instance: format!("retroft-{}", std::process::id()),
            owner_file: None,
            buffer_kb_per_cpu: vec![4096],
            subbuffer_kb: None,
            syscalls: false,
            irqs: false,
            events: [
                "sched_switch",
                "sched_waking",
                "sched_wakeup",
                "sched_process_fork",
                "sched_process_exec",
                "sched_process_exit",
            ]
            .iter()
            .map(|s| format!("sched:{s}"))
            .collect(),
            optional_events: Vec::new(),
            trace_clock: "mono".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub system: String,
    pub name: String,
    pub id: u32,
    pub format_ref: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub tid: u32,
    pub tgid: u32,
    pub comm: String,
}
#[derive(Debug, Clone)]
pub struct MetadataFile {
    pub path: String,
    pub bytes: Vec<u8>,
}
#[derive(Debug, Serialize)]
pub struct TraceMetadata {
    pub schema_version: u32,
    pub architecture: String,
    pub kernel_release: String,
    pub endianness: String,
    pub word_size: usize,
    pub page_size: usize,
    pub subbuffer_size: usize,
    pub trace_clock: String,
    pub cpus: Vec<u32>,
    pub enabled_events: Vec<String>,
    pub events: Vec<EventMetadata>,
    pub header_page_ref: String,
    pub header_event_ref: String,
    pub initial_tasks: Vec<TaskInfo>,
    pub task_snapshot_truncated: bool,
    pub task_snapshot_note: &'static str,
    pub clock_monotonic_ns: String,
    pub clock_boottime_ns: String,
    pub clock_sample_uncertainty_ns: String,
    pub kernel_buffer_bytes: String,
    pub backend_kernel_bytes: String,
    pub userspace_accumulator_bytes: String,
    pub backend: String,
    pub diagnostics: Vec<String>,
    #[serde(skip)]
    pub files: Vec<MetadataFile>,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadInfo {
    pub len: usize,
    pub base_timestamp_ns: u64,
    /// Committed event bytes after the raw page header.
    pub committed_bytes: usize,
    /// Reserved for format compatibility; snapshot reads always start at zero.
    pub skip_bytes: usize,
    /// None means loss was flagged but the kernel supplied no exact count.
    pub lost_events: Option<u64>,
    /// Kept separately because an absent count still confirms loss.
    pub has_lost_events: bool,
}

#[derive(Clone, Copy)]
struct PageLayout {
    timestamp_offset: usize,
    commit_offset: usize,
    word_size: usize,
    data_offset: usize,
}

struct CpuReader {
    id: u32,
    capacity_bytes: u64,
    file: File,
    stats_file: File,
    // Assemble a short raw read without confusing it with a partial kernel page.
    partial: Vec<u8>,
    partial_len: usize,
}

pub struct TraceSession {
    // Held through reader closure and instance cleanup, including setup errors.
    _owner: Option<File>,
    finished: bool,
    instance_path: PathBuf,
    readers: Vec<CpuReader>,
    cpus: Vec<u32>,
    metadata: TraceMetadata,
    layout: PageLayout,
    subbuffer_size: usize,
}

/// Keeps setup transactional: even a failed configuration removes owned state.
struct InstanceGuard {
    path: PathBuf,
    armed: bool,
}
impl Drop for InstanceGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = cleanup_instance(&self.path);
        }
    }
}

/// Discover instead of hardcoding syscall names: availability and layouts belong
/// to the recording kernel. Feed each event through normal metadata collection.
fn named_syscall_events(instance: &Path) -> Result<Vec<String>> {
    let directory = instance.join("events/syscalls");
    let entries = fs::read_dir(&directory)
        .context("--syscalls requires named syscall events (CONFIG_FTRACE_SYSCALLS)")?;
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if (name.starts_with("sys_enter_") || name.starts_with("sys_exit_"))
            && entry.file_type()?.is_dir()
        {
            names.push(format!("syscalls:{name}"));
        }
    }
    names.sort();
    ensure!(
        !names.is_empty(),
        "--syscalls found no named syscall events (CONFIG_FTRACE_SYSCALLS is required)"
    );
    // A partial pair would silently lose either arguments or completion data.
    for name in &names {
        let partner = if let Some(call) = name.strip_prefix("syscalls:sys_enter_") {
            format!("syscalls:sys_exit_{call}")
        } else {
            format!(
                "syscalls:sys_enter_{}",
                name.strip_prefix("syscalls:sys_exit_").unwrap()
            )
        };
        ensure!(
            names.binary_search(&partner).is_ok(),
            "--syscalls: {name} is missing matching event {partner}"
        );
    }
    Ok(names)
}

// Linux UAPI membarrier commands. GLOBAL currently uses synchronize_rcu() on
// SMP; expedited commands do not provide the kernel-reader wait we need.
const MEMBARRIER_CMD_QUERY: libc::c_int = 0;
const MEMBARRIER_CMD_GLOBAL: libc::c_int = 1;

fn membarrier(command: libc::c_int) -> io::Result<libc::c_long> {
    // No pointers are passed; flags and the unused CPU argument must be zero.
    let result = unsafe { libc::syscall(libc::SYS_membarrier, command, 0u32, 0i32) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn require_snapshot_barrier() -> Result<()> {
    let supported = membarrier(MEMBARRIER_CMD_QUERY)
        .context("query snapshot writer barrier; CONFIG_MEMBARRIER is required")?;
    ensure!(supported & libc::c_long::from(MEMBARRIER_CMD_GLOBAL) != 0,
        "snapshot writer barrier requires MEMBARRIER_CMD_GLOBAL (unavailable with nohz_full); no timed fallback is used");
    Ok(())
}

fn snapshot_barrier() -> Result<()> {
    // This relies on Linux's GLOBAL implementation, not the userspace memory
    // ordering contract alone. See docs/kernel-notes.md for compatibility limits.
    membarrier(MEMBARRIER_CMD_GLOBAL).context("MEMBARRIER_CMD_GLOBAL")?;
    Ok(())
}

impl TraceSession {
    pub fn start(config: &TraceConfig) -> Result<Self> {
        if !valid_component(&config.instance) {
            bail!("invalid tracefs instance name");
        }
        if config.buffer_kb_per_cpu.is_empty() || config.buffer_kb_per_cpu.contains(&0) {
            bail!("buffer sizes must be nonempty and positive");
        }
        require_snapshot_barrier()?;
        verify_tracefs_root(&config.tracefs)?;
        if !matches!(config.trace_clock.as_str(), "mono" | "boot") {
            bail!("only mono and boot trace clocks are supported; local/global clocks cannot be correlated with client monotonic endpoints");
        }
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            bail!("cannot determine system page size");
        }
        let page_size = page_size as usize;
        let instance_path = config.tracefs.join("instances").join(&config.instance);
        let mut diagnostics = Vec::new();
        let recovering = config.owner_file.is_some() && instance_path.exists();
        let owner = prepare_owned_instance(config.owner_file.as_deref(), &instance_path)?;
        if recovering {
            diagnostics.push(format!(
                "removed crashed owned tracefs instance {}",
                instance_path.display()
            ));
        }
        fs::create_dir(&instance_path).with_context(|| format!("create exclusive tracefs instance {} (requires accessible tracefs and tracing privileges; existing instances require a matching --instance-owner reservation)", instance_path.display()))?;
        let mut guard = InstanceGuard {
            path: instance_path.clone(),
            armed: true,
        };
        write_setting(&instance_path, "tracing_on", "0")?;
        write_setting(&instance_path, "current_tracer", "nop")?;
        write_setting(&instance_path, "events/enable", "0")?;
        write_setting(&instance_path, "options/overwrite", "1")?;
        let subbuffer_fallback = if let Some(kb) = config.subbuffer_kb {
            let size = kb.checked_mul(1024).context("sub-buffer size overflow")?;
            if size < page_size || size % page_size != 0 || !(size / page_size).is_power_of_two() {
                bail!("sub-buffer size must be a power-of-two multiple of system page size");
            }
            write_setting(&instance_path, "buffer_subbuf_size_kb", &kb.to_string())
                .context("kernel does not support requested sub-buffer configuration")?;
            None
        } else {
            // Larger chunks amortize reads and compression. Older kernels or
            // allocation failures must not make the automatic default fatal.
            let preferred_kb = (64 * 1024).max(page_size) / 1024;
            write_setting(
                &instance_path,
                "buffer_subbuf_size_kb",
                &preferred_kb.to_string(),
            )
            .err()
        };
        write_setting(&instance_path, "trace_clock", &config.trace_clock)?;
        let clocks = read_bounded(&instance_path.join("trace_clock"), 4096)?;
        let selected = format!("[{}]", config.trace_clock);
        if !String::from_utf8_lossy(&clocks)
            .split_whitespace()
            .any(|s| s == selected)
        {
            bail!(
                "kernel did not select requested trace clock {}",
                config.trace_clock
            );
        }
        let subbuffer_size = match fs::read_to_string(instance_path.join("buffer_subbuf_size_kb")) {
            Ok(s) => s
                .trim()
                .parse::<usize>()
                .context("invalid kernel sub-buffer size")?
                .checked_mul(1024)
                .context("kernel sub-buffer size overflow")?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => page_size,
            Err(e) => return Err(e).context("read tracefs sub-buffer size"),
        };
        if let Some(error) = subbuffer_fallback {
            diagnostics.push(format!("sub-buffer default unavailable: {error:#}; using kernel size {subbuffer_size} bytes"));
        }
        // Reject implausible kernel geometry before allocating per-CPU scratch.
        if subbuffer_size < page_size
            || subbuffer_size > 16 * 1024 * 1024
            || subbuffer_size % page_size != 0
        {
            bail!("invalid kernel sub-buffer geometry");
        }
        let mut files = Vec::new();
        let mut metadata_size = 0;
        for name in ["events/header_page", "events/header_event"] {
            add_metadata_file(
                &instance_path,
                &config.tracefs,
                name,
                &mut files,
                &mut metadata_size,
            )?;
        }
        let header_page = &files[0].bytes;
        let layout = PageLayout::parse(header_page, subbuffer_size)?;
        let mut events = Vec::new();
        let mut enabled_events = Vec::new();
        let syscall_events = if config.syscalls {
            named_syscall_events(&instance_path)?
        } else {
            Vec::new()
        };
        let irq_events = if config.irqs {
            [
                "irq:irq_handler_entry",
                "irq:irq_handler_exit",
                "irq:softirq_raise",
                "irq:softirq_entry",
                "irq:softirq_exit",
            ]
            .map(str::to_owned)
            .to_vec()
        } else {
            Vec::new()
        };
        for (optional, event) in config
            .events
            .iter()
            .chain(syscall_events.iter())
            .map(|e| (false, e))
            .chain(
                config
                    .optional_events
                    .iter()
                    .chain(irq_events.iter())
                    .map(|e| (true, e)),
            )
        {
            let (system, name) = event
                .split_once(':')
                .context("event must have system:name format")?;
            if !valid_component(system) || !valid_component(name) {
                bail!("invalid event {event}");
            }
            if enabled_events.contains(event) {
                continue;
            }
            let event_dir = format!("events/{system}/{name}");
            if optional
                && !instance_path
                    .join(&event_dir)
                    .try_exists()
                    .context("inspect optional trace event")?
            {
                diagnostics.push(format!("optional event unavailable: {event}"));
                continue;
            }
            let format_ref = format!("{event_dir}/format");
            add_metadata_file(
                &instance_path,
                &config.tracefs,
                &format_ref,
                &mut files,
                &mut metadata_size,
            )?;
            let text = std::str::from_utf8(&files.last().unwrap().bytes)
                .context("event format is not UTF-8")?;
            let id = text
                .lines()
                .find_map(|l| l.strip_prefix("ID:"))
                .context("event format missing ID")?
                .trim()
                .parse()
                .context("invalid event ID")?;
            write_setting(&instance_path, &format!("{event_dir}/enable"), "1")?;
            events.push(EventMetadata {
                system: system.into(),
                name: name.into(),
                id,
                format_ref,
            });
            enabled_events.push(event.clone());
        }
        if events.is_empty() {
            bail!("at least one event must be enabled");
        }
        let mut cpus = Vec::new();
        for entry in fs::read_dir(instance_path.join("per_cpu"))? {
            let entry = entry?;
            if let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|n| n.strip_prefix("cpu"))
                .and_then(|n| n.parse::<u32>().ok())
            {
                cpus.push(id);
            }
        }
        cpus.sort_unstable();
        // Tracefs creates directories for possible CPUs even when they have no
        // allocated ring buffer yet. The recorder captures CPUs online at startup.
        let online =
            fs::read_to_string("/sys/devices/system/cpu/online").context("discover online CPUs")?;
        let online = parse_cpu_list(&online)?;
        cpus.retain(|cpu| online.contains(cpu));
        diagnostics
            .push("CPU set is fixed to CPUs online at startup; restart after CPU hotplug".into());
        if cpus.is_empty() {
            bail!("no per-CPU raw buffers available");
        }
        if cpus != online {
            bail!("tracefs CPU set {cpus:?} does not match online CPUs {online:?}; retry after CPU topology is stable");
        }
        configure_buffers(&instance_path, &cpus, &config.buffer_kb_per_cpu)?;
        // Allocate the spare before tracing starts; later swaps never allocate rings.
        write_setting(&instance_path, "snapshot", "1")
            .context("allocate snapshot bank; CONFIG_TRACER_SNAPSHOT is required")?;
        let mut readers = Vec::with_capacity(cpus.len());
        for &id in &cpus {
            let path = instance_path.join(format!("per_cpu/cpu{id}/snapshot_raw"));
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
                .open(&path)
                .with_context(|| format!("open {}", path.display()))?;
            let stats_file = File::open(instance_path.join(format!("per_cpu/cpu{id}/stats")))
                .with_context(|| format!("open CPU {id} tracing statistics"))?;
            let capacity_bytes =
                fs::read_to_string(instance_path.join(format!("per_cpu/cpu{id}/buffer_size_kb")))?
                    .trim()
                    .parse::<u64>()?
                    .checked_mul(1024)
                    .context("buffer accounting overflow")?;
            readers.push(CpuReader {
                id,
                capacity_bytes,
                file,
                stats_file,
                partial: vec![0; subbuffer_size],
                partial_len: 0,
            });
        }
        let (initial_tasks, task_snapshot_truncated) = snapshot_tasks()?;
        let mono_before = clock_ns(libc::CLOCK_MONOTONIC)?;
        let boot = clock_ns(libc::CLOCK_BOOTTIME)?;
        let mono_after = clock_ns(libc::CLOCK_MONOTONIC)?;
        let kernel_buffer_bytes = readers.iter().try_fold(0u64, |sum, reader| {
            sum.checked_add(
                reader
                    .capacity_bytes
                    .checked_mul(2)
                    .context("buffer accounting overflow")?,
            )
            .and_then(|bytes| bytes.checked_add(2 * subbuffer_size as u64))
            .context("buffer accounting overflow")
        })?;
        let metadata = TraceMetadata {
            schema_version: 1,
            architecture: std::env::consts::ARCH.into(),
            kernel_release: fs::read_to_string("/proc/sys/kernel/osrelease")
                .context("read kernel release")?.trim().to_owned(),
            endianness: if cfg!(target_endian = "little") { "little" } else { "big" }.into(),
            word_size: layout.word_size,
            page_size, subbuffer_size, trace_clock: config.trace_clock.clone(), cpus: cpus.clone(), enabled_events, events,
            header_page_ref: "events/header_page".into(), header_event_ref: "events/header_event".into(),
            initial_tasks, task_snapshot_truncated,
            task_snapshot_note: "Best-effort /proc snapshot before tracing begins; tasks may exit, rename, or fork during enumeration. Lifecycle events supplement this snapshot.",
            clock_monotonic_ns: (mono_before + (mono_after - mono_before) / 2).to_string(),
            clock_boottime_ns: boot.to_string(), clock_sample_uncertainty_ns: (mono_after - mono_before).to_string(),
            kernel_buffer_bytes: kernel_buffer_bytes.to_string(),
            backend_kernel_bytes: (cpus.len() * subbuffer_size).to_string(),
            userspace_accumulator_bytes: (cpus.len() * subbuffer_size).to_string(),
            backend: "snapshot-read".into(), diagnostics, files,
        };
        write_setting(&instance_path, "tracing_on", "1")?;
        guard.armed = false;
        Ok(Self {
            _owner: owner,
            finished: false,
            instance_path,
            readers,
            cpus,
            metadata,
            layout,
            subbuffer_size,
        })
    }

    pub fn cpu_ids(&self) -> &[u32] {
        &self.cpus
    }
    pub fn subbuffer_size(&self) -> usize {
        self.subbuffer_size
    }
    pub fn backend_name(&self) -> &str {
        &self.metadata.backend
    }
    pub fn metadata(&self) -> &TraceMetadata {
        &self.metadata
    }
    pub fn instance_path(&self) -> &Path {
        &self.instance_path
    }

    /// The caller must fully consume the previous snapshot before swapping.
    /// Wait for pre-swap writers before exposing the frozen bank to readers.
    /// Consuming reads leave it reusable without a destructive reset.
    pub fn swap_snapshot(&mut self) -> Result<()> {
        self.swap_snapshot_with_barrier(snapshot_barrier)
    }

    fn swap_snapshot_with_barrier(&mut self, barrier: impl FnOnce() -> Result<()>) -> Result<()> {
        if self.readers.iter().any(|reader| reader.partial_len != 0) {
            bail!("cannot swap snapshot with an incomplete raw sub-buffer");
        }
        write_setting(&self.instance_path, "snapshot", "1")?;
        barrier().context("wait for snapshot writers after swap")?;
        // Raw descriptors follow the snapshot buffer; their consuming reader
        // positions travel with each bank, including partly consumed pages.
        Ok(())
    }

    /// First and maximum actual event timestamps, excluding framing controls.
    pub fn event_bounds(&self, bytes: &[u8], info: ReadInfo) -> Result<Option<(u64, u64)>> {
        self.layout.event_bounds(bytes, info)
    }

    /// Read a padded sub-buffer from the frozen bank, including partial pages.
    pub fn read(&mut self, cpu: u32, buffer: &mut [u8]) -> Result<Option<ReadInfo>> {
        if buffer.len() < self.subbuffer_size {
            bail!("staging block smaller than trace sub-buffer");
        }
        let index = self
            .cpus
            .binary_search(&cpu)
            .map_err(|_| anyhow::anyhow!("unknown trace CPU {cpu}"))?;
        self.read_raw(index, buffer)
    }

    fn read_raw(&mut self, index: usize, buffer: &mut [u8]) -> Result<Option<ReadInfo>> {
        let cpu = self.cpus[index];
        let reader = &mut self.readers[index];
        // A normal successful raw read is a complete padded sub-buffer. Preserve
        // a rare short read internally until the fd's remaining page is read.
        loop {
            let destination = if reader.partial_len == 0 {
                &mut buffer[..self.subbuffer_size]
            } else {
                &mut reader.partial[reader.partial_len..]
            };
            let count = unsafe {
                libc::read(
                    reader.file.as_raw_fd(),
                    destination.as_mut_ptr().cast(),
                    destination.len(),
                )
            };
            if count < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if error.kind() == io::ErrorKind::WouldBlock {
                    return Ok(None);
                }
                return Err(error).context(format!("read CPU {cpu} raw trace"));
            }
            if count == 0 {
                if reader.partial_len > 0 {
                    bail!("CPU {cpu} raw stream ended within a sub-buffer");
                }
                return Ok(None);
            }
            let count = count as usize;
            if reader.partial_len == 0 && count == self.subbuffer_size {
                return self
                    .layout
                    .info(&buffer[..count], count, 0)
                    .map(Some)
                    .with_context(|| format!("decode CPU {cpu} raw page"));
            }
            if reader.partial_len == 0 {
                reader.partial[..count].copy_from_slice(&buffer[..count]);
            }
            reader.partial_len += count;
            if reader.partial_len == self.subbuffer_size {
                buffer[..self.subbuffer_size].copy_from_slice(&reader.partial);
                reader.partial_len = 0;
                return self
                    .layout
                    .info(&buffer[..self.subbuffer_size], self.subbuffer_size, 0)
                    .map(Some);
            }
        }
    }

    /// Check the live bank without consuming it. `bytes` is unconsumed data,
    /// not a lifetime production counter; the counters follow each swapped bank.
    pub fn active_bank_needs_drain(&self) -> Result<bool> {
        active_bank_needs_drain(&self.readers)
    }

    pub fn stats_for_serialization(&self) -> impl Serialize + '_ {
        StatsJson {
            readers: &self.readers,
        }
    }

    /// Summarize the live bank without consuming its retained history.
    pub fn idle_status(&self) -> Result<Vec<CpuIdleStatus>> {
        idle_status(&self.readers)
    }

    /// Report teardown errors to the embedding caller. Drop also attempts cleanup
    /// on early returns, without printing or replacing the original error.
    pub fn finish(&mut self) -> Result<()> {
        let stopped = write_setting(&self.instance_path, "tracing_on", "0");
        // Close readers even if disabling tracing failed; the owner lock must
        // stay held until these references and the cleanup attempt are finished.
        self.readers.clear();
        let cleaned = cleanup_instance(&self.instance_path);
        self.finished = cleaned.is_ok();
        stopped.and(cleaned)
    }
}

fn active_bank_needs_drain(readers: &[CpuReader]) -> Result<bool> {
    let mut scratch = [0u8; STATS_CAPACITY];
    for reader in readers {
        let raw = stats_text(&reader.stats_file, &mut scratch)
            .with_context(|| format!("read active buffer occupancy for CPU {}", reader.id))?;
        let bytes = BorrowedCpuStats::from_text(reader.id, raw)
            .bytes
            .and_then(stats_counter)
            .context("CPU statistics lack a valid bytes counter")?;
        // Any busy CPU triggers a swap; averaging would hide its overflow risk.
        if bytes >= reader.capacity_bytes.div_ceil(2) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn stats_text<'a>(file: &File, scratch: &'a mut [u8; STATS_CAPACITY]) -> Result<&'a str> {
    let mut file = file;
    file.seek(SeekFrom::Start(0))?;
    // One read avoids joining fragments from independently generated snapshots.
    let used = loop {
        match file.read(scratch) {
            Ok(count) => break count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    };
    if used == scratch.len() {
        bail!("CPU statistics exceed 16 KiB limit");
    }
    Ok(std::str::from_utf8(&scratch[..used])?)
}

fn stats_counter(value: &str) -> Option<u64> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn stats_seconds_ns(value: &str) -> Option<u64> {
    // Decimal parsing avoids rounding large kernel timestamps through f64.
    let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
    if value.ends_with('.') || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let fraction = fraction.trim_end_matches('0');
    if fraction.len() > 9 {
        return None;
    }
    let nanos = if fraction.is_empty() {
        0
    } else {
        stats_counter(fraction)?
    };
    stats_counter(seconds)?
        .checked_mul(1_000_000_000)?
        .checked_add(nanos.checked_mul(10u64.pow(9 - fraction.len() as u32))?)
}

// Display roughly three significant digits; exact counters remain in captures.
fn compact(value: f64, unit: &str) -> String {
    let decimals = if value >= 99.95 {
        0
    } else if value >= 9.995 {
        1
    } else {
        2
    };
    format!("{value:.decimals$}{unit}")
}

fn compact_count(count: u128) -> String {
    let mut value = count as f64;
    for unit in ["", "k", "M", "G", "T", "P", "E"] {
        if value < 999.5 || unit == "E" {
            return if unit.is_empty() {
                count.to_string()
            } else {
                compact(value, unit)
            };
        }
        value /= 1000.0;
    }
    unreachable!()
}

fn compact_age(ns: u64) -> String {
    let (scale, unit) = match ns {
        0 => return "0s".into(),
        1..1_000 => (1.0, "ns"),
        1_000..1_000_000 => (1e3, "us"),
        1_000_000..1_000_000_000 => (1e6, "ms"),
        1_000_000_000..1_000_000_000_000 => (1e9, "s"),
        1_000_000_000_000..3_600_000_000_000 => (60e9, "m"),
        3_600_000_000_000..86_400_000_000_000 => (3600e9, "h"),
        _ => (86400e9, "d"),
    };
    compact(ns as f64 / scale, unit)
}

fn idle_status(readers: &[CpuReader]) -> Result<Vec<CpuIdleStatus>> {
    let mut scratch = [0u8; STATS_CAPACITY];
    let mut result = Vec::with_capacity(readers.len());
    for reader in readers {
        let raw = stats_text(&reader.stats_file, &mut scratch)
            .with_context(|| format!("read idle statistics for CPU {}", reader.id))?;
        let stats = BorrowedCpuStats::from_text(reader.id, raw);
        let entries = stats.entries.and_then(stats_counter);
        let age = entries.filter(|count| *count > 0).and_then(|_| {
            stats
                .now_ts
                .and_then(stats_seconds_ns)?
                .checked_sub(stats.oldest_event_ts.and_then(stats_seconds_ns)?)
        });
        result.push(CpuIdleStatus {
            cpu: reader.id,
            entries,
            oldest_age_ns: age,
        });
    }
    Ok(result)
}

#[cfg(test)]
fn report_idle(readers: &[CpuReader], output: &mut impl Write) -> Result<()> {
    write_idle_status(&idle_status(readers)?, output)
}

/// Human-readable rendering used by the CLI; integrations consume the fields.
pub fn write_idle_status(status: &[CpuIdleStatus], output: &mut impl Write) -> Result<()> {
    let mut ages = Vec::with_capacity(status.len());
    let mut cpus = Vec::new();
    let mut entries_total = 0u128;
    let mut entries_known = true;
    let (mut empty, mut unknown) = (0, 0);
    for cpu in status {
        let entries = cpu.entries;
        let age = cpu.oldest_age_ns;
        if let Some(entries) = entries {
            entries_total += u128::from(entries);
        } else {
            entries_known = false;
        }
        if !cpus.is_empty() {
            write!(cpus, " ")?;
        }
        write!(cpus, "{}:", cpu.cpu)?;
        if entries == Some(0) {
            empty += 1;
            write!(cpus, "-")?;
        } else if let Some(age) = age {
            ages.push(age);
            write!(cpus, "{}", compact_age(age))?;
        } else {
            unknown += 1;
            write!(cpus, "?")?;
        }
    }
    ages.sort_unstable();
    write!(output, "idle: history min/median/max=")?;
    if let (Some(min), Some(max)) = (ages.first(), ages.last()) {
        let middle = ages.len() / 2;
        // Difference-first averaging avoids overflowing nanosecond timestamps.
        let median = if ages.len() % 2 == 0 {
            ages[middle - 1] + (ages[middle] - ages[middle - 1]) / 2
        } else {
            ages[middle]
        };
        write!(
            output,
            "{}/{}/{}",
            compact_age(*min),
            compact_age(median),
            compact_age(*max)
        )?;
    } else {
        write!(output, "?")?;
    }
    write!(
        output,
        " events={}",
        if entries_known {
            compact_count(entries_total)
        } else {
            "?".into()
        }
    )?;
    if empty > 0 {
        write!(output, " empty={empty}")?;
    }
    if unknown > 0 {
        write!(output, " unknown={unknown}")?;
    }
    write!(output, " cpu-age=[")?;
    output.write_all(&cpus)?;
    writeln!(output, "]")?;
    Ok(())
}

struct StatsJson<'a> {
    readers: &'a [CpuReader],
}

#[derive(Serialize)]
struct BorrowedCpuStats<'a> {
    cpu: u32,
    entries: Option<&'a str>,
    overrun: Option<&'a str>,
    commit_overrun: Option<&'a str>,
    dropped_events: Option<&'a str>,
    read_events: Option<&'a str>,
    bytes: Option<&'a str>,
    oldest_event_ts: Option<&'a str>,
    now_ts: Option<&'a str>,
    raw: &'a str,
}

impl<'a> BorrowedCpuStats<'a> {
    fn from_text(cpu: u32, raw: &'a str) -> Self {
        let value = |key: &str| {
            raw.lines().find_map(|line| {
                line.split_once(':')
                    .filter(|(name, _)| *name == key)
                    .map(|(_, value)| value.trim())
            })
        };
        Self {
            cpu,
            entries: value("entries"),
            overrun: value("overrun"),
            commit_overrun: value("commit overrun"),
            dropped_events: value("dropped events"),
            read_events: value("read events"),
            bytes: value("bytes"),
            oldest_event_ts: value("oldest event ts"),
            now_ts: value("now ts"),
            raw,
        }
    }
}

impl Serialize for StatsJson<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.readers.len()))?;
        let mut scratch = [0u8; STATS_CAPACITY];
        for reader in self.readers {
            let raw =
                stats_text(&reader.stats_file, &mut scratch).map_err(serde::ser::Error::custom)?;
            sequence.serialize_element(&BorrowedCpuStats::from_text(reader.id, raw))?;
        }
        sequence.end()
    }
}

impl Drop for TraceSession {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish();
        }
    }
}

/// An owner file reserves one canonical instance path until it is
/// removed. Its flock prevents a second recorder from touching a live session.
fn prepare_owned_instance(owner_path: Option<&Path>, instance: &Path) -> Result<Option<File>> {
    let Some(owner_path) = owner_path else {
        return Ok(None);
    };
    let canonical_parent = instance
        .parent()
        .context("instance has no parent")?
        .canonicalize()?;
    let canonical_instance =
        canonical_parent.join(instance.file_name().context("instance has no name")?);
    let mut expected = b"RETROFT-INSTANCE-OWNER1\n".to_vec();
    expected.extend_from_slice(canonical_instance.as_os_str().as_bytes());
    if expected.len() > 4096 {
        bail!("instance owner path exceeds marker limit");
    }
    let mut owner = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(owner_path)
        .with_context(|| format!("open instance owner {}", owner_path.display()))?;
    if unsafe { libc::flock(owner.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::last_os_error())
            .context("instance owner is locked by another recorder");
    }
    if !owner.metadata()?.is_file() {
        bail!("instance owner must be a regular file");
    }
    let mut marker = Vec::new();
    Read::by_ref(&mut owner)
        .take(4097)
        .read_to_end(&mut marker)?;
    let exists = instance.try_exists()?;
    if marker != expected {
        // A process killed while writing its initial intent never created the
        // instance. Complete only a matching partial intent with no instance.
        if exists || !expected.starts_with(&marker) {
            bail!(
                "instance owner does not match {}; refusing takeover",
                instance.display()
            );
        }
        owner.seek(SeekFrom::Start(0))?;
        owner.write_all(&expected)?;
        owner.set_len(expected.len() as u64)?;
    }
    if exists {
        // Do not swallow cleanup errors: starting over a still-active ring would
        // mix ownership and lose the guarantee provided by the exclusive lock.
        write_setting(instance, "tracing_on", "0")?;
        write_setting(instance, "events/enable", "0")?;
        fs::remove_dir(instance)
            .with_context(|| format!("remove crashed owned instance {}", instance.display()))?;
    }
    Ok(Some(owner))
}

fn cleanup_instance(path: &Path) -> Result<()> {
    // Attempt every cleanup operation, then report an error to the caller.
    let stopped = write_setting(path, "tracing_on", "0");
    let disabled = write_setting(path, "events/enable", "0");
    let removed = fs::remove_dir(path)
        .with_context(|| format!("remove owned tracefs instance {}", path.display()));
    removed?;
    stopped?;
    disabled
}

fn valid_component(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

fn parse_cpu_list(list: &str) -> Result<Vec<u32>> {
    let mut cpus = Vec::new();
    for part in list.trim().split(',') {
        let (first, last) = if let Some((first, last)) = part.split_once('-') {
            (first.parse::<u32>()?, last.parse::<u32>()?)
        } else {
            let cpu = part.parse::<u32>()?;
            (cpu, cpu)
        };
        if first > last || last > 1_048_576 {
            bail!("invalid online CPU range");
        }
        cpus.extend(first..=last);
    }
    Ok(cpus)
}
fn buffer_sizes(cpus: &[u32], sizes: &[usize]) -> Result<Vec<(u32, usize)>> {
    if sizes.is_empty() || sizes.contains(&0) {
        bail!("buffer sizes must be nonempty and positive");
    }
    if sizes.len() != 1 && sizes.len() != cpus.len() {
        bail!("--buffer-per-cpu has {} sizes; expected one size or {} sizes for online CPUs {cpus:?} in ascending ID order", sizes.len(), cpus.len());
    }
    Ok(cpus
        .iter()
        .enumerate()
        .map(|(index, &cpu)| (cpu, sizes[if sizes.len() == 1 { 0 } else { index }]))
        .collect())
}

fn configure_buffers(root: &Path, cpus: &[u32], sizes: &[usize]) -> Result<()> {
    // Validate the complete CPU-to-size mapping before changing any buffer.
    let plan = buffer_sizes(cpus, sizes)?;
    if sizes.len() == 1 {
        write_setting(root, "buffer_size_kb", &sizes[0].to_string())?;
    } else {
        for (cpu, size) in plan {
            write_setting(
                root,
                &format!("per_cpu/cpu{cpu}/buffer_size_kb"),
                &size.to_string(),
            )?;
        }
    }
    Ok(())
}

fn write_setting(root: &Path, name: &str, value: &str) -> Result<()> {
    fs::write(root.join(name), value.as_bytes())
        .with_context(|| format!("configure tracefs {name}={value}"))
}
fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    read_metadata(file, limit).with_context(|| format!("read {}", path.display()))
}

fn read_metadata(reader: impl Read, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    // tracefs header_page/header_event return EOF after their first read, even
    // if it was short. Buffer the underlying read so Vec's small initial reads
    // cannot truncate those descriptions (which fit within one kernel page).
    io::BufReader::with_capacity(64 * 1024, reader)
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        bail!("exceeds metadata limit {limit}");
    }
    Ok(bytes)
}
fn add_metadata_file(
    instance: &Path,
    tracefs: &Path,
    name: &str,
    files: &mut Vec<MetadataFile>,
    size: &mut usize,
) -> Result<()> {
    let path = if instance
        .join(name)
        .try_exists()
        .context("inspect trace metadata file")?
    {
        instance.join(name)
    } else {
        tracefs.join(name)
    };
    let bytes = read_bounded(&path, MAX_METADATA - *size)?;
    *size += bytes.len();
    files.push(MetadataFile {
        path: name.into(),
        bytes,
    });
    Ok(())
}

fn verify_tracefs_root(path: &Path) -> Result<()> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)
        .with_context(|| {
            format!(
                "open tracefs root {} (mount tracefs and grant tracing privileges)",
                path.display()
            )
        })?;
    let mut information = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::fstatfs(directory.as_raw_fd(), information.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error()).context("identify tracefs filesystem");
    }
    let information = unsafe { information.assume_init() };
    // TRACEFS_MAGIC and DEBUGFS_MAGIC; older kernels expose tracing in debugfs.
    if !matches!(information.f_type as u64, 0x7472_6163 | 0x6462_6720) {
        bail!(
            "{} is not tracefs or the legacy debugfs tracing mount",
            path.display()
        );
    }
    Ok(())
}

fn snapshot_tasks() -> Result<(Vec<TaskInfo>, bool)> {
    let mut tasks = Vec::new();
    for process in fs::read_dir("/proc")? {
        let process = match process {
            Ok(p) => p,
            Err(_) => continue,
        };
        let tgid = match process
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u32>().ok())
        {
            Some(id) => id,
            None => continue,
        };
        let entries = match fs::read_dir(process.path().join("task")) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for thread in entries {
            let thread = match thread {
                Ok(t) => t,
                Err(_) => continue,
            };
            let tid = match thread
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u32>().ok())
            {
                Some(id) => id,
                None => continue,
            };
            if tasks.len() == MAX_TASKS {
                return Ok((tasks, true));
            }
            let comm = match read_bounded(&thread.path().join("comm"), 256) {
                Ok(b) => String::from_utf8_lossy(&b)
                    .trim_end_matches('\n')
                    .to_string(),
                Err(_) => continue,
            };
            tasks.push(TaskInfo { tid, tgid, comm });
        }
    }
    tasks.sort_unstable_by_key(|t| t.tid);
    Ok((tasks, false))
}

impl PageLayout {
    // Walk only the ring framing: trimming must account for timestamp controls,
    // but does not need event schemas or allocate decoded event objects.
    fn event_bounds(&self, bytes: &[u8], info: ReadInfo) -> Result<Option<(u64, u64)>> {
        let end = self
            .data_offset
            .checked_add(info.committed_bytes)
            .context("committed region overflow")?;
        let data = bytes
            .get(self.data_offset..end)
            .context("committed region exceeds page")?;
        if info.skip_bytes > data.len() || !info.skip_bytes.is_multiple_of(4) {
            bail!("invalid duplicate prefix");
        }
        let word = |offset: usize| -> Result<u32> {
            let end = offset.checked_add(4).context("ring offset overflow")?;
            Ok(u32::from_ne_bytes(
                data.get(offset..end)
                    .context("ring record exceeds committed region")?
                    .try_into()
                    .unwrap(),
            ))
        };
        let mut timestamp = info.base_timestamp_ns;
        let mut cursor = 0;
        let mut bounds: Option<(u64, u64)> = None;
        let mut skip_is_boundary = info.skip_bytes == 0;
        while cursor < data.len() {
            skip_is_boundary |= cursor == info.skip_bytes;
            let header = word(cursor)?;
            let (kind, delta) = if cfg!(target_endian = "little") {
                (header & 31, header >> 5)
            } else {
                (header >> 27, header & ((1 << 27) - 1))
            };
            let start = cursor;
            cursor += 4;
            match kind {
                30 | 31 => {
                    let extended = (u64::from(word(cursor)?) << 27) | u64::from(delta);
                    cursor += 4;
                    timestamp = if kind == 31 {
                        // Absolute controls carry only 59 bits; restore the
                        // page's epoch, including a rollover within the page.
                        let high = timestamp & (0xf8u64 << 56);
                        let absolute = high | extended;
                        if high != 0 && absolute < timestamp {
                            absolute
                                .checked_add(1u64 << 59)
                                .context("absolute timestamp rollover")?
                        } else {
                            absolute
                        }
                    } else {
                        timestamp
                            .checked_add(extended)
                            .context("timestamp overflow")?
                    };
                }
                29 => {
                    if delta == 0 {
                        cursor = data.len();
                    } else {
                        let length = word(cursor)? as usize;
                        if length < 4 || !length.is_multiple_of(4) {
                            bail!("invalid padding length");
                        }
                        cursor = cursor.checked_add(length).context("padding overflow")?;
                        timestamp = timestamp
                            .checked_add(u64::from(delta))
                            .context("timestamp overflow")?;
                    }
                }
                0..=28 => {
                    timestamp = timestamp
                        .checked_add(u64::from(delta))
                        .context("timestamp overflow")?;
                    let length = if kind == 0 {
                        let length = word(cursor)? as usize;
                        if length < 4 {
                            bail!("invalid extended data length");
                        }
                        cursor += 4;
                        (length - 4).checked_add(3).context("length overflow")? & !3
                    } else {
                        kind as usize * 4
                    };
                    if length < 2 {
                        bail!("empty data event");
                    }
                    cursor = cursor
                        .checked_add(length)
                        .context("event length overflow")?;
                    if cursor > data.len() {
                        bail!("event exceeds committed region");
                    }
                    if start >= info.skip_bytes {
                        bounds = Some(match bounds {
                            None => (timestamp, timestamp),
                            Some((first, maximum)) => (first, maximum.max(timestamp)),
                        });
                    }
                }
                _ => unreachable!(),
            }
            if cursor > data.len() {
                bail!("ring record exceeds committed region");
            }
        }
        if !skip_is_boundary && info.skip_bytes != data.len() {
            bail!("duplicate prefix is not a ring-record boundary");
        }
        Ok(bounds)
    }

    fn parse(bytes: &[u8], subbuffer_size: usize) -> Result<Self> {
        let text = std::str::from_utf8(bytes).context("header_page is not UTF-8")?;
        let field = |name: &str| -> Result<(usize, usize)> {
            let line = text
                .lines()
                .find(|l| {
                    l.split(';')
                        .next()
                        .is_some_and(|f| f.trim_end().ends_with(&format!(" {name}")))
                })
                .with_context(|| format!("header_page missing {name}"))?;
            let number = |prefix: &str| -> Result<usize> {
                line.split(';')
                    .find_map(|p| p.trim().strip_prefix(prefix))
                    .with_context(|| {
                        format!("header_page field {name:?} missing {prefix:?}: {line:?}")
                    })?
                    .trim()
                    .parse()
                    .with_context(|| {
                        format!("header_page field {name:?} has invalid {prefix:?}: {line:?}")
                    })
            };
            Ok((number("offset:")?, number("size:")?))
        };
        let (timestamp_offset, timestamp_size) = field("timestamp")?;
        let (commit_offset, word_size) = field("commit")?;
        let (data_offset, data_size) = field("data")?;
        if timestamp_size != 8
            || !matches!(word_size, 4 | 8)
            || timestamp_offset + 8 > data_offset
            || commit_offset + word_size > data_offset
            || data_offset + data_size != subbuffer_size
        {
            bail!("unsupported raw ftrace header_page layout");
        }
        Ok(Self {
            timestamp_offset,
            commit_offset,
            word_size,
            data_offset,
        })
    }
    fn native_word(&self, bytes: &[u8]) -> u64 {
        if self.word_size == 4 {
            u32::from_ne_bytes(bytes[..4].try_into().unwrap()) as u64
        } else {
            u64::from_ne_bytes(bytes[..8].try_into().unwrap())
        }
    }
    fn info(&self, bytes: &[u8], len: usize, skip_bytes: usize) -> Result<ReadInfo> {
        let word = self.native_word(&bytes[self.commit_offset..]);
        let committed_bytes = (word & COMMIT_LENGTH_MASK) as usize;
        if committed_bytes > len - self.data_offset || skip_bytes > committed_bytes {
            bail!("invalid raw ftrace committed length: commit={word:#018x} committed={committed_bytes} page_bytes={len} data_offset={} skip={skip_bytes}", self.data_offset);
        }
        let lost_events = if word & (1 << 31) == 0 {
            Some(0)
        } else if word & (1 << 30) != 0
            && self.data_offset + committed_bytes + self.word_size <= len
        {
            Some(self.native_word(&bytes[self.data_offset + committed_bytes..]))
        } else {
            None
        };
        Ok(ReadInfo {
            len,
            base_timestamp_ns: u64::from_ne_bytes(
                bytes[self.timestamp_offset..self.timestamp_offset + 8]
                    .try_into()
                    .unwrap(),
            ),
            committed_bytes,
            skip_bytes,
            lost_events,
            has_lost_events: page_has_loss(word, skip_bytes),
        })
    }
}

// Preserve the format rule for historical mmap continuation records.
fn page_has_loss(header: u64, skip_bytes: usize) -> bool {
    skip_bytes == 0 && header & (1 << 31) != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_syscalls_discover_pairs_and_reject_missing_support() -> Result<()> {
        let dir = tempfile::tempdir()?;
        assert!(named_syscall_events(dir.path()).is_err());
        let events = dir.path().join("events/syscalls");
        fs::create_dir_all(&events)?;
        fs::write(events.join("enable"), "0")?;
        assert!(named_syscall_events(dir.path()).is_err());
        for name in [
            "sys_exit_write",
            "sys_enter_read",
            "sys_enter_write",
            "sys_exit_read",
        ] {
            fs::create_dir(events.join(name))?;
        }
        assert_eq!(
            named_syscall_events(dir.path())?,
            [
                "syscalls:sys_enter_read",
                "syscalls:sys_enter_write",
                "syscalls:sys_exit_read",
                "syscalls:sys_exit_write",
            ]
        );
        fs::create_dir(events.join("sys_enter_openat"))?;
        assert!(named_syscall_events(dir.path())
            .unwrap_err()
            .to_string()
            .contains("sys_exit_openat"));
        Ok(())
    }

    fn bounds_fixture(base: u64, words: &[u32]) -> (PageLayout, Vec<u8>, ReadInfo) {
        let layout = PageLayout {
            timestamp_offset: 0,
            commit_offset: 8,
            word_size: 8,
            data_offset: 16,
        };
        let mut bytes = vec![0; 16];
        for word in words {
            bytes.extend_from_slice(&word.to_ne_bytes());
        }
        let info = ReadInfo {
            len: bytes.len(),
            base_timestamp_ns: base,
            committed_bytes: words.len() * 4,
            skip_bytes: 0,
            lost_events: Some(0),
            has_lost_events: false,
        };
        (layout, bytes, info)
    }
    fn ring_header(kind: u32, delta: u32) -> u32 {
        if cfg!(target_endian = "little") {
            kind | (delta << 5)
        } else {
            (kind << 27) | delta
        }
    }
    #[test]
    fn event_bounds_follow_controls_and_keep_crossing_pages() {
        let (layout, bytes, info) = bounds_fixture(
            10,
            &[
                ring_header(30, 1000),
                0,
                ring_header(1, 2),
                0,
                ring_header(1, 100),
                0,
            ],
        );
        assert_eq!(
            layout.event_bounds(&bytes, info).unwrap(),
            Some((1012, 1112))
        );
        let (layout, bytes, info) = bounds_fixture(10, &[ring_header(30, 1000), 0]);
        assert_eq!(layout.event_bounds(&bytes, info).unwrap(), None);
    }
    #[test]
    fn event_bounds_use_maximum_when_absolute_time_goes_backwards() {
        let (layout, bytes, info) = bounds_fixture(
            100,
            &[
                ring_header(1, 0),
                0,
                ring_header(31, 20),
                0,
                ring_header(1, 0),
                0,
            ],
        );
        assert_eq!(layout.event_bounds(&bytes, info).unwrap(), Some((100, 100)));
        let (layout, bytes, info) = bounds_fixture(
            (2u64 << 59) - 10,
            &[ring_header(31, 7), 0, ring_header(1, 0), 0],
        );
        assert_eq!(
            layout.event_bounds(&bytes, info).unwrap(),
            Some(((2u64 << 59) + 7, (2u64 << 59) + 7))
        );
    }
    #[test]
    fn event_bounds_validate_lengths_padding_and_continuations() {
        let (mut layout, bytes, mut info) = bounds_fixture(
            10,
            &[
                ring_header(0, 1),
                8,
                0,
                ring_header(29, 2),
                4,
                ring_header(1, 3),
                0,
            ],
        );
        // A 32-bit header layout uses the same event framing.
        layout.word_size = 4;
        assert_eq!(layout.event_bounds(&bytes, info).unwrap(), Some((11, 16)));
        info.skip_bytes = 12;
        assert_eq!(layout.event_bounds(&bytes, info).unwrap(), Some((16, 16)));
        info.skip_bytes = 4;
        assert!(layout.event_bounds(&bytes, info).is_err());
        for words in [
            vec![ring_header(30, 1)],
            vec![ring_header(1, 1)],
            vec![ring_header(29, 1), 100],
            vec![ring_header(0, 1), 3],
        ] {
            let (layout, bytes, info) = bounds_fixture(0, &words);
            assert!(layout.event_bounds(&bytes, info).is_err());
        }
        let (layout, bytes, info) = bounds_fixture(u64::MAX, &[ring_header(1, 1), 0]);
        assert!(layout.event_bounds(&bytes, info).is_err());
    }
    #[test]
    fn failed_teardown_still_closes_raw_readers() -> Result<()> {
        let (_directory, mut session) = raw_session(&[])?;
        // A regular directory cannot emulate tracefs control-file removal.
        // Even if teardown fails, no reader references may outlive the lock.
        assert!(session.finish().is_err());
        assert!(session.readers.is_empty());
        Ok(())
    }

    fn raw_session(pages: &[(u64, bool)]) -> Result<(tempfile::TempDir, TraceSession)> {
        // Keep cleanup confined to a fixture instance, never the working directory.
        let directory = tempfile::tempdir()?;
        let instance_path = directory.path().join("instance");
        fs::create_dir(&instance_path)?;
        let mut file = tempfile::tempfile()?;
        for &(timestamp, lost) in pages {
            let mut page = [0u8; 64];
            page[..8].copy_from_slice(&timestamp.to_ne_bytes());
            page[8..16].copy_from_slice(&(4u64 | if lost { 1 << 31 } else { 0 }).to_ne_bytes());
            file.write_all(&page)?;
        }
        file.seek(SeekFrom::Start(0))?;
        let reader = CpuReader {
            id: 0,
            capacity_bytes: 4096,
            file,
            stats_file: tempfile::tempfile()?,
            partial: vec![0; 64],
            partial_len: 0,
        };
        let session = TraceSession {
            _owner: None,
            finished: false,
            instance_path,
            readers: vec![reader],
            cpus: vec![0],
            subbuffer_size: 64,
            layout: PageLayout {
                timestamp_offset: 0,
                commit_offset: 8,
                word_size: 8,
                data_offset: 16,
            },
            metadata: TraceMetadata {
                schema_version: 1,
                architecture: String::new(),
                kernel_release: "6.1.0".into(),
                endianness: String::new(),
                word_size: 8,
                page_size: 64,
                subbuffer_size: 64,
                trace_clock: String::new(),
                cpus: vec![0],
                enabled_events: vec![],
                events: vec![],
                header_page_ref: String::new(),
                header_event_ref: String::new(),
                initial_tasks: vec![],
                task_snapshot_truncated: false,
                task_snapshot_note: "",
                clock_monotonic_ns: String::new(),
                clock_boottime_ns: String::new(),
                clock_sample_uncertainty_ns: String::new(),
                kernel_buffer_bytes: String::new(),
                backend_kernel_bytes: String::new(),
                userspace_accumulator_bytes: String::new(),
                backend: String::new(),
                diagnostics: vec![],
                files: vec![],
            },
        };
        Ok((directory, session))
    }

    #[test]
    fn owner_intent_survives_restart_and_lock_excludes_another_recorder() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let instance = directory.path().join("instance");
        let owner_path = directory.path().join("owner");
        let owner = prepare_owned_instance(Some(&owner_path), &instance)?;
        let marker = fs::read(&owner_path)?;
        assert!(marker.starts_with(b"RETROFT-INSTANCE-OWNER1\n"));
        assert!(prepare_owned_instance(Some(&owner_path), &instance).is_err());
        drop(owner);
        let restarted = prepare_owned_instance(Some(&owner_path), &instance)?;
        assert_eq!(fs::read(&owner_path)?, marker);
        drop(restarted);
        Ok(())
    }

    #[test]
    fn owner_refuses_unmarked_or_mismatched_existing_instances() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let instance = directory.path().join("unrelated");
        let owner_path = directory.path().join("owner");
        fs::create_dir(&instance)?;
        fs::write(instance.join("tracing_on"), b"1")?;
        assert!(prepare_owned_instance(Some(&owner_path), &instance).is_err());
        assert_eq!(fs::read(instance.join("tracing_on"))?, b"1");
        fs::write(&owner_path, b"another owner's marker")?;
        assert!(prepare_owned_instance(Some(&owner_path), &instance).is_err());
        assert_eq!(fs::read(instance.join("tracing_on"))?, b"1");
        Ok(())
    }

    #[test]
    fn owned_restart_disables_events_and_propagates_removal_failure() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let instance = directory.path().join("instance");
        let owner_path = directory.path().join("owner");
        drop(prepare_owned_instance(Some(&owner_path), &instance)?);
        fs::create_dir_all(instance.join("events"))?;
        fs::write(instance.join("tracing_on"), b"1")?;
        fs::write(instance.join("events/enable"), b"1")?;
        // A regular directory cannot model tracefs's virtual-file rmdir. It
        // deliberately makes removal fail, which startup must not ignore.
        assert!(prepare_owned_instance(Some(&owner_path), &instance).is_err());
        assert_eq!(fs::read(instance.join("tracing_on"))?, b"0");
        assert_eq!(fs::read(instance.join("events/enable"))?, b"0");
        Ok(())
    }

    #[test]
    fn continuation_does_not_repeat_prior_loss() {
        assert!(page_has_loss(1 << 31, 0));
        assert!(!page_has_loss(1 << 31, 32));
        assert!(!page_has_loss(4, 0));
    }

    #[test]
    fn frozen_reads_preserve_leading_history_and_loss_without_lookahead() -> Result<()> {
        let (_directory, mut session) = raw_session(&[(1, false), (2, true), (3, false)])?;
        let mut out = [0; 64];
        for (timestamp, lost) in [(1, false), (2, true), (3, false)] {
            let page = session.read(0, &mut out)?.unwrap();
            assert_eq!(page.base_timestamp_ns, timestamp);
            assert_eq!(page.has_lost_events, lost);
            assert_eq!(page.skip_bytes, 0);
        }
        assert!(session.read(0, &mut out)?.is_none());
        Ok(())
    }

    #[test]
    fn snapshot_swap_waits_after_swap_without_clearing() -> Result<()> {
        let (_directory, mut session) = raw_session(&[])?;
        let setting = session.instance_path.join("snapshot");
        for _ in 0..2 {
            fs::write(&setting, "unchanged")?;
            session.swap_snapshot_with_barrier(|| {
                assert_eq!(fs::read_to_string(&setting)?, "1");
                Ok(())
            })?;
            assert_eq!(fs::read_to_string(&setting)?, "1");
        }
        assert!(session
            .swap_snapshot_with_barrier(|| anyhow::bail!("barrier failed"))
            .is_err());
        Ok(())
    }

    #[test]
    fn swap_refuses_unfinished_raw_page_before_touching_snapshot() -> Result<()> {
        let (_directory, mut session) = raw_session(&[])?;
        session.readers[0].partial_len = 1;
        assert!(session
            .swap_snapshot_with_barrier(|| panic!("must not reach barrier"))
            .unwrap_err()
            .to_string()
            .contains("incomplete"));
        assert!(!session.instance_path.join("snapshot").exists());
        Ok(())
    }

    #[test]
    fn global_barrier_completes_when_supported() -> Result<()> {
        require_snapshot_barrier()?;
        snapshot_barrier()
    }

    #[test]
    fn metadata_headers_survive_single_read_tracefs_files() -> Result<()> {
        // Model the kernel header reader: the first read advances its offset;
        // every subsequent read returns EOF, regardless of bytes left unread.
        struct HeaderFile(Option<&'static [u8]>);
        impl Read for HeaderFile {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                let bytes = self.0.take().unwrap_or_default();
                let len = out.len().min(bytes.len());
                out[..len].copy_from_slice(&bytes[..len]);
                Ok(len)
            }
        }
        let header: &[u8] = b"\tfield: u64 timestamp;\toffset:0;\tsize:8;\tsigned:0;\n\tfield: local_t commit;\toffset:8;\tsize:8;\tsigned:1;\n\tfield: char overwrite;\toffset:8;\tsize:1;\tsigned:0;\n\tfield: char data;\toffset:16;\tsize:4080;\tsigned:0;\n";
        let bytes = read_metadata(HeaderFile(Some(header)), MAX_METADATA)?;
        assert_eq!(bytes, header);
        PageLayout::parse(&bytes, 4096)?;
        assert!(read_metadata(HeaderFile(Some(header)), header.len() - 1).is_err());
        assert_eq!(
            read_metadata(HeaderFile(Some(header)), header.len())?,
            header
        );
        Ok(())
    }

    #[test]
    fn raw_layout_and_loss_flags() {
        let layout = PageLayout::parse(b"field: u64 timestamp; offset:0; size:8; signed:0;\nfield: local_t commit; offset:8; size:8; signed:1;\nfield: char data; offset:16; size:4080; signed:1;\n", 4096).unwrap();
        let mut page = vec![0u8; 4096];
        page[..8].copy_from_slice(&123u64.to_ne_bytes());
        page[8..16].copy_from_slice(&(16u64 | (3u64 << 30)).to_ne_bytes());
        page[32..40].copy_from_slice(&42u64.to_ne_bytes());
        let info = layout.info(&page, page.len(), 8).unwrap();
        assert_eq!(info.base_timestamp_ns, 123);
        assert_eq!(info.committed_bytes, 16);
        assert_eq!(info.skip_bytes, 8);
        assert_eq!(info.lost_events, Some(42));
        // local_add(RB_MISSED_EVENTS, ...) promotes a signed int to long.
        // Both stored-count and flag-only pages occur on real 64-bit kernels.
        for (flags, expected_loss) in [(1u32 << 31, None), (3u32 << 30, Some(42))] {
            let signed_flags = i64::from(flags as i32) as u64;
            page[8..16].copy_from_slice(&(16 | signed_flags).to_ne_bytes());
            let info = layout.info(&page, page.len(), 0).unwrap();
            assert_eq!(info.committed_bytes, 16);
            assert!(info.has_lost_events);
            assert_eq!(info.lost_events, expected_loss);
        }
        page[8..16].copy_from_slice(&5000u64.to_ne_bytes());
        assert!(layout.info(&page, page.len(), 0).is_err());
    }
    #[test]
    fn online_cpu_ranges() {
        assert_eq!(
            parse_cpu_list("0-2,4,8-9\n").unwrap(),
            vec![0, 1, 2, 4, 8, 9]
        );
        assert!(parse_cpu_list("3-1").is_err());
    }

    #[test]
    fn per_cpu_sizes_map_sparse_ids_and_validate_before_writes() -> Result<()> {
        let cpus = [0, 2, 7];
        assert_eq!(
            buffer_sizes(&cpus, &[4096])?,
            vec![(0, 4096), (2, 4096), (7, 4096)]
        );
        assert_eq!(
            buffer_sizes(&cpus, &[1024, 2048, 4096])?,
            vec![(0, 1024), (2, 2048), (7, 4096)]
        );
        assert!(buffer_sizes(&cpus, &[]).is_err());
        assert!(buffer_sizes(&cpus, &[0]).is_err());
        let root = tempfile::tempdir()?;
        for cpu in cpus {
            fs::create_dir_all(root.path().join(format!("per_cpu/cpu{cpu}")))?;
            fs::write(
                root.path().join(format!("per_cpu/cpu{cpu}/buffer_size_kb")),
                "old",
            )?;
        }
        fs::write(root.path().join("buffer_size_kb"), "global")?;
        assert!(configure_buffers(root.path(), &cpus, &[1, 2]).is_err());
        assert_eq!(
            fs::read_to_string(root.path().join("per_cpu/cpu0/buffer_size_kb"))?,
            "old"
        );
        configure_buffers(root.path(), &cpus, &[1024, 2048, 4096])?;
        for (cpu, size) in [(0, "1024"), (2, "2048"), (7, "4096")] {
            assert_eq!(
                fs::read_to_string(root.path().join(format!("per_cpu/cpu{cpu}/buffer_size_kb")))?,
                size
            );
        }
        assert_eq!(
            fs::read_to_string(root.path().join("buffer_size_kb"))?,
            "global"
        );
        configure_buffers(root.path(), &cpus, &[8192])?;
        assert_eq!(
            fs::read_to_string(root.path().join("buffer_size_kb"))?,
            "8192"
        );
        Ok(())
    }

    #[test]
    fn occupancy_uses_each_cpus_capacity_and_refreshes_stats() -> Result<()> {
        use std::io::Write;
        let mut readers = Vec::new();
        for (id, capacity, bytes) in [(0, 8192, 100), (1, 4096, 2047)] {
            let mut stats = tempfile::tempfile()?;
            writeln!(stats, "bytes: {bytes}")?;
            readers.push(CpuReader {
                id,
                capacity_bytes: capacity,
                file: stats.try_clone()?,
                stats_file: stats,
                partial: Vec::new(),
                partial_len: 0,
            });
        }
        assert!(!active_bank_needs_drain(&readers)?);
        readers[1].stats_file.rewind()?;
        readers[1].stats_file.set_len(0)?;
        readers[1].stats_file.write_all(b"bytes: 2048\n")?;
        assert!(active_bank_needs_drain(&readers)?);
        readers[1].stats_file.rewind()?;
        readers[1].stats_file.set_len(0)?;
        readers[1].stats_file.write_all(b"bytes: 0\n")?;
        assert!(!active_bank_needs_drain(&readers)?);
        readers[1].stats_file.set_len(0)?;
        assert!(active_bank_needs_drain(&readers).is_err());
        Ok(())
    }

    #[test]
    fn bounded_stats_json_reuses_descriptor_and_preserves_large_counters() -> Result<()> {
        use std::io::Write;
        let mut stats = tempfile::tempfile()?;
        stats.write_all(
            b"entries: 18446744073709551615\noverrun: 42\noldest event ts: 1.000002\n",
        )?;
        let reader = CpuReader {
            id: 7,
            capacity_bytes: 4096,
            file: stats.try_clone()?,
            stats_file: stats,
            partial: Vec::new(),
            partial_len: 0,
        };
        let readers = [reader];
        let value = StatsJson { readers: &readers };
        let mut buffer = [0u8; 1024];
        let encode = |out: &mut [u8]| -> Result<usize> {
            let mut cursor = io::Cursor::new(out);
            serde_json::to_writer(&mut cursor, &value)?;
            Ok(cursor.position() as usize)
        };
        let first = encode(&mut buffer)?;
        let parsed: serde_json::Value = serde_json::from_slice(&buffer[..first])?;
        assert_eq!(parsed[0]["cpu"], 7);
        assert_eq!(parsed[0]["entries"], "18446744073709551615");
        assert_eq!(parsed[0]["overrun"], "42");
        assert!(parsed[0]["dropped_events"].is_null());
        assert!(parsed[0]["raw"].as_str().unwrap().contains('\n'));
        assert_eq!(encode(&mut buffer)?, first);
        assert!(encode(&mut buffer[..8]).is_err());
        Ok(())
    }

    fn idle_reader(cpu: u32, text: &[u8]) -> Result<CpuReader> {
        let mut raw = tempfile::tempfile()?;
        raw.write_all(b"raw-data-stays-unread")?;
        raw.seek(SeekFrom::Start(4))?;
        let mut stats = tempfile::tempfile()?;
        stats.write_all(text)?;
        Ok(CpuReader {
            id: cpu,
            capacity_bytes: 4096,
            file: raw,
            stats_file: stats,
            partial: b"untouched".to_vec(),
            partial_len: 9,
        })
    }

    #[test]
    fn idle_report_summarizes_retention() -> Result<()> {
        let readers = [
            idle_reader(
                0,
                b"entries: 100000\noverrun: 1000000\noldest event ts: 0\nnow ts: 20.123\n",
            )?,
            idle_reader(
                1,
                b"entries: 80000\noverrun: 420000\noldest event ts: 0\nnow ts: 49.229\n",
            )?,
            idle_reader(
                2,
                b"entries: 51000\noverrun: 0\noldest event ts: 0\nnow ts: 223.512\n",
            )?,
            idle_reader(3, b"entries: 0\noverrun: 0\n")?,
        ];
        let mut report = Vec::new();
        report_idle(&readers, &mut report)?;
        assert_eq!(String::from_utf8(report)?, "idle: history min/median/max=20.1s/49.2s/224s events=231k empty=1 cpu-age=[0:20.1s 1:49.2s 2:224s 3:-]\n");
        assert_eq!(compact_age(123_456), "123us");
        assert_eq!(compact_age(1_234_567), "1.23ms");
        assert_eq!(compact_age(12_345_678_000), "12.3s");
        assert_eq!(compact_age(3_600_000_000_000), "1.00h");
        Ok(())
    }

    #[test]
    fn idle_report_uses_exact_same_file_times_and_does_not_consume_raw() -> Result<()> {
        let readers = [
            idle_reader(0, b"entries: 18446744073709551615\noldest event ts: 9007199.254740\nnow ts: 9007219.378196\n")?,
            idle_reader(2, b"entries: 0\noldest event ts: missing\n")?,
            idle_reader(7, b"entries: 123\noldest event ts: 12.999999\nnow ts: 14.000000\n")?,
        ];
        let mut report = Vec::new();
        report_idle(&readers, &mut report)?;
        let expected = "idle: history min/median/max=1.00s/10.6s/20.1s events=18.4E empty=1 cpu-age=[0:20.1s 2:- 7:1.00s]\n";
        assert_eq!(std::str::from_utf8(&report)?, expected);
        let mut again = Vec::new();
        report_idle(&readers, &mut again)?;
        assert_eq!(again, report);
        for reader in &readers {
            let mut raw = &reader.file;
            assert_eq!(raw.stream_position()?, 4);
            assert_eq!(reader.partial, b"untouched");
            assert_eq!(reader.partial_len, 9);
            let mut remaining = Vec::new();
            raw.read_to_end(&mut remaining)?;
            assert_eq!(remaining, b"data-stays-unread");
        }
        // Nanoseconds beyond floating-point integer precision remain exact.
        assert_eq!(
            stats_seconds_ns("9007199.254740993"),
            Some(9_007_199_254_740_993)
        );
        assert_eq!(stats_seconds_ns("18446744073.709551615"), Some(u64::MAX));
        assert_eq!(stats_seconds_ns("18446744073.709551616"), None);
        Ok(())
    }

    #[test]
    fn idle_report_missing_malformed_and_skewed_fields_are_unknown() -> Result<()> {
        for text in [
            "entries: 1\n",
            "entries: 1\noldest event ts: 20.0\nnow ts: 19.0\n",
            "entries: 1\noldest event ts: -1.0\nnow ts: 20.0\n",
            "entries: 1\noldest event ts: 1.0\nnow ts: NaN\n",
            "entries: 1\noldest event ts: 1.0000000001\nnow ts: 2.0\n",
            "entries: 1\noldest event ts: 1.0\nnow ts: 18446744074.0\n",
        ] {
            let readers = [idle_reader(3, text.as_bytes())?];
            let mut output = Vec::new();
            report_idle(&readers, &mut output)?;
            assert_eq!(
                String::from_utf8(output)?,
                "idle: history min/median/max=? events=1 unknown=1 cpu-age=[3:?]\n"
            );
        }
        for text in [
            "",
            "entries: invalid\n",
            "entries: -1\n",
            "entries: 18446744073709551616\n",
        ] {
            let readers = [idle_reader(3, text.as_bytes())?];
            let mut output = Vec::new();
            report_idle(&readers, &mut output)?;
            assert_eq!(
                String::from_utf8(output)?,
                "idle: history min/median/max=? events=? unknown=1 cpu-age=[3:?]\n"
            );
        }
        for malformed in [
            "",
            "-1",
            "NaN",
            "inf",
            "1.",
            "1..0",
            ".1",
            "+1",
            "1.0000000001",
        ] {
            assert_eq!(stats_seconds_ns(malformed), None, "accepted {malformed}");
        }
        assert_eq!(stats_seconds_ns("1.0000000000"), Some(1_000_000_000));
        assert_eq!(stats_seconds_ns("1.5"), Some(1_500_000_000));
        Ok(())
    }

    #[test]
    fn idle_report_propagates_io_and_bounded_stats_errors() -> Result<()> {
        let mut reader = idle_reader(0, b"entries: 1\n")?;
        reader.stats_file = File::open("/dev/null")?;
        let mut output = Vec::new();
        // /dev/null seeks/reads successfully and has absent fields.
        report_idle(&[reader], &mut output)?;
        let mut unreadable = idle_reader(1, b"entries: 1\n")?;
        unreadable.stats_file = OpenOptions::new()
            .write(true)
            .open(tempfile::NamedTempFile::new()?.path())?;
        assert!(report_idle(&[unreadable], &mut Vec::new()).is_err());
        let too_large = idle_reader(2, &vec![b'x'; STATS_CAPACITY])?;
        assert!(report_idle(&[too_large], &mut Vec::new()).is_err());
        Ok(())
    }
}
