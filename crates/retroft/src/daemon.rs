//! Single-threaded trigger, drain and completion loop. No async runtime.
use crate::readiness::{Deadline, Readiness};
use crate::trigger::{
    PeerIdentity, TriggerRead, TriggerRequest, TriggerSource, MAX_DESCRIPTION, MAX_SOURCE,
};
#[cfg(test)]
use crate::trigger::{UnixSocket as Socket, MAX_MESSAGE};
use crate::{
    clock::{ClockMonitor, ClockSnapshot, Reason, SNAPSHOT_RESERVE},
    storage::Storage,
    tracefs::{TraceConfig, TraceSession},
};
use crate::{status::TriggerStatus, Shutdown, StatusEvent};
use anyhow::{ensure, Context, Result};
use retroft_format::{self as format, RecordHeader, RecordKind, HEADER_LEN};
use serde::Serialize;
#[cfg(test)]
use std::os::unix::net::UnixDatagram;
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::fd::{AsFd, AsRawFd},
    path::PathBuf,
    time::{Duration, Instant},
};

// Small built-in vocabulary: no external word list or naming dependency.
const SESSION_WORDS: [&str; 256] = [
    "acorn",
    "amber",
    "anchor",
    "anvil",
    "apple",
    "apron",
    "arrow",
    "atlas",
    "autumn",
    "avocado",
    "badger",
    "bagel",
    "balloon",
    "bamboo",
    "banana",
    "barley",
    "basil",
    "basket",
    "bat",
    "beacon",
    "bear",
    "beaver",
    "beetle",
    "bell",
    "bench",
    "berry",
    "birch",
    "biscuit",
    "blossom",
    "boat",
    "bonnet",
    "bottle",
    "boulder",
    "bowl",
    "branch",
    "bread",
    "breeze",
    "bridge",
    "brook",
    "brush",
    "bucket",
    "buffalo",
    "butterfly",
    "button",
    "cabbage",
    "cabin",
    "cactus",
    "camel",
    "candle",
    "canoe",
    "canvas",
    "canyon",
    "caper",
    "captain",
    "carpet",
    "carrot",
    "cashew",
    "castle",
    "cedar",
    "cello",
    "chalk",
    "cheese",
    "cherry",
    "chest",
    "chick",
    "chisel",
    "cinnamon",
    "cloud",
    "clover",
    "cobble",
    "cocoa",
    "comet",
    "cookie",
    "copper",
    "coral",
    "cotton",
    "coyote",
    "crane",
    "crepe",
    "cricket",
    "crown",
    "cucumber",
    "cushion",
    "dahlia",
    "daisy",
    "dancer",
    "delta",
    "desert",
    "diamond",
    "dingo",
    "dolly",
    "dolphin",
    "domino",
    "donkey",
    "dove",
    "dragon",
    "drift",
    "drum",
    "duck",
    "dune",
    "eagle",
    "earth",
    "echo",
    "egg",
    "elm",
    "ember",
    "engine",
    "fabric",
    "falcon",
    "feather",
    "fern",
    "fiddle",
    "fig",
    "finch",
    "firefly",
    "flag",
    "flamingo",
    "flower",
    "flute",
    "forest",
    "fossil",
    "fountain",
    "fox",
    "frost",
    "gadget",
    "galaxy",
    "garden",
    "garlic",
    "gazelle",
    "gem",
    "ginger",
    "giraffe",
    "glacier",
    "globe",
    "goat",
    "goblet",
    "gold",
    "goose",
    "gopher",
    "gourd",
    "grape",
    "gravel",
    "grove",
    "guitar",
    "hammock",
    "hamster",
    "harbor",
    "hawk",
    "hazel",
    "helmet",
    "heron",
    "hickory",
    "hill",
    "hippo",
    "honey",
    "hound",
    "iceberg",
    "igloo",
    "ink",
    "iris",
    "island",
    "ivy",
    "jacket",
    "jaguar",
    "jasmine",
    "jelly",
    "jigsaw",
    "juniper",
    "kayak",
    "kernel",
    "kettle",
    "kingfisher",
    "kite",
    "kiwi",
    "koala",
    "ladder",
    "lake",
    "lamb",
    "lantern",
    "lark",
    "leaf",
    "leek",
    "lemon",
    "leopard",
    "lettuce",
    "lilac",
    "lime",
    "linen",
    "lizard",
    "llama",
    "lobster",
    "lotus",
    "magnet",
    "magnolia",
    "mandarin",
    "mango",
    "mantis",
    "maple",
    "marble",
    "marmot",
    "meadow",
    "melon",
    "merlin",
    "meteor",
    "mirror",
    "mitten",
    "monkey",
    "moose",
    "moss",
    "mountain",
    "mouse",
    "muffin",
    "mulberry",
    "mushroom",
    "narwhal",
    "necklace",
    "nettle",
    "noodle",
    "nutmeg",
    "oak",
    "ocean",
    "octopus",
    "olive",
    "onion",
    "opal",
    "orange",
    "orchid",
    "osprey",
    "otter",
    "oyster",
    "paddle",
    "panda",
    "paper",
    "parrot",
    "peach",
    "pebble",
    "pepper",
    "petal",
    "piano",
    "pickle",
    "rabbit",
    "raven",
    "ribbon",
    "river",
    "robin",
    "rocket",
    "saddle",
    "salmon",
    "sandal",
    "shell",
    "silver",
    "sparrow",
    "spoon",
    "spruce",
    "stone",
    "waffle",
];

// ISO 8601 basic UTC names are for people; the full random key in metadata
// prevents joining unrelated sessions even if their date and words match.
fn utc_filename_time(time: std::time::SystemTime) -> String {
    humantime::format_rfc3339_seconds(time)
        .to_string()
        .replace([':', '-'], "")
}

fn new_session_id(started: std::time::SystemTime) -> Result<(String, String, String)> {
    let timestamp = utc_filename_time(started);
    let mut random = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut random)?;
    let words = format!(
        "{}-{}-{}",
        SESSION_WORDS[usize::from(random[0])],
        SESSION_WORDS[usize::from(random[1])],
        SESSION_WORDS[usize::from(random[2])],
    );
    Ok((
        format!("{timestamp}_{words}"),
        words,
        format!("{:032x}", u128::from_le_bytes(random)),
    ))
}

// A UTF-8 description can expand to six JSON bytes per input byte.
// Includes escaped source/process names and peer credentials.
const TRIGGER_RESERVE: usize = (MAX_DESCRIPTION + MAX_SOURCE + 256) * 6 + 512;
#[non_exhaustive]
pub struct Config {
    pub trace: TraceConfig,
    pub output: PathBuf,
    /// Upper bound for payload buffers, excluding io_uring and other metadata.
    pub staging_bytes: usize,
    /// Stored file bytes per batch, synchronized before the next batch is written.
    pub sync_batch_bytes: u64,
    /// Optional period for non-consuming buffer reports while idle.
    pub idle_report: Option<Duration>,
    /// Extra history before the latest first event backed by leading loss.
    /// None disables trimming.
    pub history_margin: Option<Duration>,
    /// None disables compression; Some supplies the uncompressed reset interval.
    pub compression: Option<usize>,
}

impl Config {
    /// Recording defaults, with an explicit existing output path.
    /// Override the settings your integration needs; leave the rest at their defaults.
    pub fn new(output: PathBuf) -> Self {
        Self {
            trace: TraceConfig::default(),
            output,
            staging_bytes: 8 * 1024 * 1024,
            sync_batch_bytes: 8 * 1024 * 1024,
            idle_report: None,
            history_margin: Some(Duration::from_secs(1)),
            compression: if cfg!(feature = "lz4") {
                Some(4 * 1024 * 1024)
            } else {
                None
            },
        }
    }
}

pub fn monotonic_ns() -> Result<u64> {
    crate::clock::clock_ns(libc::CLOCK_MONOTONIC)
}

struct Trigger {
    // Assigned on admission; independent of the record numbers within a file.
    sequence: u64,
    timestamp_ns: u64,
    end_timestamp_ns: u64,
    description: String,
    source: Option<String>,
    peer: Option<PeerIdentity>,
}
impl Trigger {
    fn status(&self, endpoint: u64) -> TriggerStatus {
        TriggerStatus {
            sequence: self.sequence,
            timestamp_ns: self.timestamp_ns,
            source: self.source.clone(),
            pid: self.peer.as_ref().map(|p| p.peer_pid),
            description: self.description.clone(),
            post_remaining_ns: endpoint.saturating_sub(self.timestamp_ns),
        }
    }

    #[cfg(test)]
    fn parse(bytes: &[u8], sequence: u64, now: u64) -> Result<Self> {
        Self::admit(TriggerRequest::from_json(bytes)?, sequence, now)
    }
    fn admit(request: TriggerRequest, sequence: u64, now: u64) -> Result<Self> {
        ensure!(
            !request.source.is_empty() && request.source.len() <= MAX_SOURCE,
            "source must contain 1 through 256 bytes"
        );
        ensure!(
            request.description.len() <= MAX_DESCRIPTION,
            "description exceeds 1024 bytes"
        );
        let end_timestamp_ns = request.end_timestamp_ns.unwrap_or(now);
        ensure!(
            end_timestamp_ns.saturating_sub(now) <= 86_400_000_000_000,
            "end timestamp exceeds 24-hour request limit"
        );
        Ok(Self {
            sequence,
            timestamp_ns: now,
            end_timestamp_ns,
            description: request.description,
            source: Some(request.source),
            peer: request.peer,
        })
    }
    fn encode(&self, out: &mut [u8]) -> Result<usize> {
        #[derive(Serialize)]
        struct Json<'a> {
            schema_version: u32,
            sequence: Decimal,
            timestamp_ns: Decimal,
            end_timestamp_ns: Decimal,
            description: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            source: Option<&'a str>,
            #[serde(flatten)]
            peer: Option<&'a PeerIdentity>,
        }
        let value = Json {
            schema_version: 1,
            sequence: Decimal(self.sequence),
            timestamp_ns: Decimal(self.timestamp_ns),
            end_timestamp_ns: Decimal(self.end_timestamp_ns),
            description: &self.description,
            source: self.source.as_deref(),
            peer: self.peer.as_ref(),
        };
        let mut writer = std::io::Cursor::new(out);
        serde_json::to_writer(&mut writer, &value)?;
        Ok(writer.position() as usize)
    }
}

fn receive_trigger(source: &mut impl TriggerSource, sequence: u64) -> Result<Option<Trigger>> {
    let TriggerRead::Request(request) = source.receive()? else {
        return Ok(None);
    };
    // Malformed input ends this admission batch. Queued input remains readable
    // and will get another turn; no sequence number is consumed by rejection.
    Ok(Trigger::admit(request, sequence, monotonic_ns()?).ok())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CapturePhase {
    Draining,
    // Once End is submitted, new triggers must wait for a new capture.
    Closing,
    Syncing,
}
impl CapturePhase {
    fn sealed(self) -> bool {
        self != Self::Draining
    }
}
struct Capture {
    path: PathBuf,
    // Processing time through final sync, not the span of retained trace history.
    started: Instant,
    triggers: u64,
    // Stable reference for human-readable history cutoff reporting across extensions.
    first_trigger_ns: u64,
    capture_index: u64,
    // Furthest requested end time among triggers merged into this file.
    endpoint: u64,
    record_sequence: u64,
    // Cursor into the session's retained suspend/resume clock snapshots.
    clock_history_written: usize,
    phase: CapturePhase,
    // Lower bound on the instant the current frozen bank was detached.
    snapshot_ns: u64,
    snapshot_banks: u64,
    // Measure draining separately from the kernel swap and writer barrier.
    first_bank_ready_ns: u64,
    first_drain_ns: Option<u64>,
    post_polling_logged: bool,
    // Monotonic deadline for the next live-bank occupancy check.
    next_occupancy_ns: u64,
    next_cpu: usize,
}
// Check during post-trigger collection only, after the frozen bank is drained.
const OCCUPANCY_CHECK_NS: u64 = 100_000_000;

impl Capture {
    fn swap_due(&mut self, now_ns: u64, occupied: impl FnOnce() -> Result<bool>) -> Result<bool> {
        if now_ns >= self.endpoint {
            return Ok(true); // Endpoint coverage never waits for a fuller bank.
        }
        if now_ns < self.next_occupancy_ns {
            return Ok(false);
        }
        let swap = occupied()?;
        self.next_occupancy_ns = now_ns.saturating_add(OCCUPANCY_CHECK_NS);
        Ok(swap)
    }

    /// End seals a stream: subsequent triggers belong to the next capture.
    fn merge(&mut self, endpoint: u64) {
        if self.phase.sealed() {
            return;
        }
        self.triggers += 1;
        if endpoint > self.endpoint {
            self.endpoint = endpoint;
        }
    }
}

// Keep exact nanoseconds and counters numeric until the JSON boundary.
#[derive(Clone, Copy, Debug)]
struct Decimal(u64);
impl Serialize for Decimal {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Completion {
    SnapshotDrained,
}

#[derive(Debug, Serialize)]
struct HistoryDiscard {
    margin_ns: Decimal,
    cutoff_ns: Option<Decimal>,
    pages: u64,
    raw_bytes: Decimal,
    kernel_loss_pages: u64,
    known_lost_events: Decimal,
    unknown_loss_pages: u64,
}
impl HistoryDiscard {
    fn new(margin_ns: u64) -> Self {
        Self {
            margin_ns: Decimal(margin_ns),
            cutoff_ns: None,
            pages: 0,
            raw_bytes: Decimal(0),
            kernel_loss_pages: 0,
            known_lost_events: Decimal(0),
            unknown_loss_pages: 0,
        }
    }
    fn discarded(&mut self, info: crate::tracefs::ReadInfo) {
        self.pages += 1;
        self.raw_bytes.0 += info.len as u64;
        if info.has_lost_events {
            self.kernel_loss_pages += 1;
            if let Some(count) = info.lost_events {
                self.known_lost_events.0 = self.known_lost_events.0.saturating_add(count);
            } else {
                self.unknown_loss_pages += 1;
            }
        }
    }
}

fn add_history_manifest(
    manifest: &mut serde_json::Value,
    margin: Option<Duration>,
    cpus: usize,
    page_size: usize,
) -> Result<usize> {
    let bytes = if margin.is_some() {
        cpus.checked_mul(2)
            .and_then(|pages| pages.checked_mul(page_size))
            .context("history probe allocation overflow")?
    } else {
        0
    };
    manifest["history_margin_ns"] = margin
        .map(|margin| serde_json::Value::String(margin.as_nanos().to_string()))
        .unwrap_or(serde_json::Value::Null);
    manifest["history_probe_bytes"] = bytes.to_string().into();
    Ok(bytes)
}

/// Hold two event-bearing pages per CPU: a retained reader fragment can precede
/// the first overwrite marker. EOF ends the probe early.
/// The fixed cutoff then permits whole-page trimming without rewriting ftrace.
#[derive(Default)]
struct CpuHistory {
    pages: [Vec<u8>; 2],
    cached: [Option<crate::tracefs::ReadInfo>; 2],
    seen: usize,
    first_event: Option<u64>,
    leading_loss: bool,
    prefix_finished: bool,
    probed: bool,
}
struct HistoryTrim {
    cpus: Vec<CpuHistory>,
    probing: bool,
    margin_ns: u64,
    cutoff: Option<u64>,
}
impl HistoryTrim {
    fn new(cpus: usize, page_size: usize, margin_ns: u64) -> Self {
        Self {
            cpus: (0..cpus)
                .map(|_| CpuHistory {
                    pages: std::array::from_fn(|_| {
                        let mut page = vec![0; page_size];
                        // Touch each page at startup to avoid page faults on trigger admission.
                        for offset in (0..page_size).step_by(4096) {
                            unsafe {
                                std::ptr::write_volatile(&mut page[offset], 0);
                            }
                        }
                        page
                    }),
                    ..CpuHistory::default()
                })
                .collect(),
            probing: true,
            margin_ns,
            cutoff: None,
        }
    }
    fn reset(&mut self) {
        for cpu in &mut self.cpus {
            *cpu = CpuHistory {
                pages: std::mem::take(&mut cpu.pages),
                ..CpuHistory::default()
            };
        }
        self.probing = true;
        self.cutoff = None;
    }
    // Returns true when the read was held back, including EOF with cached data.
    fn probe(
        &mut self,
        cpu: usize,
        info: Option<crate::tracefs::ReadInfo>,
        bounds: Option<(u64, u64)>,
        raw: &[u8],
        discarded: &mut HistoryDiscard,
    ) -> bool {
        let cpu = &mut self.cpus[cpu];
        let Some(info) = info else {
            cpu.probed = true;
            // Replay cached pages before marking this CPU drained.
            return cpu.cached.iter().any(Option::is_some);
        };
        if info.has_lost_events {
            cpu.leading_loss = true;
            cpu.first_event = None;
            // The prefix before a confirmed overwrite is disconnected, even
            // when its timestamps would otherwise fall inside the trim margin.
            for cached in &mut cpu.cached {
                if let Some(page) = cached.take() {
                    discarded.discarded(page);
                }
            }
        }
        let Some((first, _)) = bounds else {
            return false; // Preserve control-only pages and keep looking.
        };
        cpu.first_event.get_or_insert(first);
        let slot = usize::from(cpu.cached[0].is_some());
        cpu.pages[slot][..info.len].copy_from_slice(&raw[..info.len]);
        cpu.cached[slot] = Some(info);
        cpu.seen += 1;
        cpu.probed = cpu.seen == 2;
        true
    }
    fn replay(&mut self, cpu: usize, raw: &mut [u8]) -> Option<crate::tracefs::ReadInfo> {
        let cpu = &mut self.cpus[cpu];
        if self.probing {
            return None;
        }
        for slot in 0..2 {
            if let Some(info) = cpu.cached[slot].take() {
                raw[..info.len].copy_from_slice(&cpu.pages[slot][..info.len]);
                return Some(info);
            }
        }
        None
    }
    fn finish_probe(&mut self, snapshot_ns: u64, cpus: &mut [CpuCaptureState]) {
        if self.probing && self.cpus.iter().all(|cpu| cpu.probed) {
            self.probing = false;
            // A sparse CPU's first event says nothing about missing history.
            // Only confirmed kernel loss establishes a usable lower boundary.
            self.cutoff = self
                .cpus
                .iter()
                .filter_map(|cpu| {
                    if cpu.leading_loss {
                        cpu.first_event
                    } else {
                        None
                    }
                })
                .max()
                .map(|first| first.min(snapshot_ns).saturating_sub(self.margin_ns));
            for cpu in cpus {
                cpu.history_discard.as_mut().unwrap().cutoff_ns = self.cutoff.map(Decimal);
            }
        }
    }
    fn discards(&mut self, cpu: usize, bank: u64, bounds: Option<(u64, u64)>) -> bool {
        let cpu = &mut self.cpus[cpu];
        if bank != 1 || cpu.prefix_finished || cpu.first_event.is_none() {
            return false;
        }
        if let Some(((_, max), cutoff)) = bounds.zip(self.cutoff) {
            if max < cutoff {
                return true;
            }
            // Keep a straddling page whole, and never trim later pages on this CPU.
            cpu.prefix_finished = true;
        }
        false
    }
}

#[derive(Debug, Serialize)]
struct CpuCaptureState {
    cpu: u32,
    first_page_timestamp_ns: Option<Decimal>,
    last_page_timestamp_ns: Option<Decimal>,
    // None means this CPU still needs draining.
    completion: Option<Completion>,
    #[serde(skip)]
    overrun_warned: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    history_discard: Option<HistoryDiscard>,
}
impl CpuCaptureState {
    fn new(cpu: u32) -> Self {
        Self {
            cpu,
            first_page_timestamp_ns: None,
            last_page_timestamp_ns: None,
            completion: None,
            overrun_warned: false,
            history_discard: None,
        }
    }
}

fn accounting<C: Serialize>(
    output: &mut [u8],
    phase: &str,
    cpus: C,
    exhaustion: Option<u64>,
    boundaries: Option<&[CpuCaptureState]>,
) -> Result<usize> {
    #[derive(Serialize)]
    struct Accounting<'a, C> {
        schema_version: u32,
        phase: &'a str,
        cpus: C,
        #[serde(skip_serializing_if = "Option::is_none")]
        staging_exhaustion_observations: Option<Decimal>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cpu_boundaries: Option<&'a [CpuCaptureState]>,
    }
    let data = Accounting {
        schema_version: 1,
        phase,
        cpus,
        staging_exhaustion_observations: exhaustion.map(Decimal),
        cpu_boundaries: boundaries,
    };
    let mut writer = std::io::Cursor::new(output);
    serde_json::to_writer(&mut writer, &data)?;
    Ok(writer.position() as usize)
}

// Pre-encoded records copied into every file, avoiding repeated serialization.
struct MetadataCache {
    bytes: Vec<u8>,
    next_sequence: u64,
    manifest: serde_json::Value,
    manifest_record_len: usize,
}
impl MetadataCache {
    fn new(records: &[(RecordKind, Vec<u8>)]) -> Result<Self> {
        ensure!(
            records.first().is_some_and(|r| r.0 == RecordKind::Metadata),
            "first cached record must be metadata"
        );
        let manifest = serde_json::from_slice(&records[0].1)?;
        let manifest_record_len = HEADER_LEN + records[0].1.len();
        let mut bytes = Vec::new();
        let mut next_sequence = 1;
        for (kind, payload) in records {
            format::write_record(
                &mut bytes,
                RecordHeader::new(*kind, next_sequence, 0),
                payload,
            )?;
            next_sequence += 1;
        }
        Ok(Self {
            bytes,
            next_sequence,
            manifest,
            manifest_record_len,
        })
    }
}

fn append_payload(
    block: &mut [u8],
    used: &mut usize,
    header: RecordHeader,
    payload: &[u8],
) -> Result<()> {
    let tail = block.get_mut(*used..).context("record block overflow")?;
    ensure!(
        tail.len() >= HEADER_LEN + payload.len(),
        "record block too small"
    );
    tail[HEADER_LEN..HEADER_LEN + payload.len()].copy_from_slice(payload);
    *used += format::seal(tail, header, payload.len(), payload.len(), 0)?;
    Ok(())
}

fn append_trigger(
    block: &mut [u8],
    used: &mut usize,
    sequence: u64,
    trigger: &Trigger,
) -> Result<()> {
    let tail = block.get_mut(*used..).context("trigger block overflow")?;
    ensure!(tail.len() >= HEADER_LEN, "trigger block too small");
    let length = trigger.encode(&mut tail[HEADER_LEN..])?;
    *used += format::seal(
        tail,
        RecordHeader::new(RecordKind::Trigger, sequence, trigger.timestamp_ns),
        length,
        length,
        0,
    )?;
    Ok(())
}

fn append_clock(
    block: &mut [u8],
    used: &mut usize,
    sequence: u64,
    snapshot: ClockSnapshot,
) -> Result<()> {
    let mut bytes = [0u8; SNAPSHOT_RESERVE];
    let length = snapshot.encode(&mut bytes)?;
    append_payload(
        block,
        used,
        RecordHeader::new(
            RecordKind::ClockSnapshot,
            sequence,
            snapshot.sample.monotonic_ns,
        ),
        &bytes[..length],
    )
}

fn clock_pending(capture: &Capture, history: &[ClockSnapshot]) -> bool {
    !capture.phase.sealed() && capture.clock_history_written < history.len()
}

fn stage_clock_history(
    storage: &mut Storage,
    capture: &mut Capture,
    history: &[ClockSnapshot],
) -> Result<bool> {
    if !clock_pending(capture, history) {
        return Ok(false);
    }
    let Some(index) = storage.free_index() else {
        return Ok(false);
    };
    let mut used = 0;
    append_clock(
        storage.buffer_mut(index),
        &mut used,
        capture.record_sequence,
        history[capture.clock_history_written],
    )?;
    storage.submit(index, used)?;
    capture.record_sequence = capture
        .record_sequence
        .checked_add(1)
        .context("record sequence exhausted")?;
    capture.clock_history_written += 1;
    Ok(true)
}

struct CaptureStart<'a> {
    trigger: &'a Trigger,
    clock: ClockSnapshot,
    capture_index: u64,
    previous_capture_index: Option<u64>,
}

fn start_capture(
    storage: &mut Storage,
    path: PathBuf,
    directory: &File,
    metadata: &MetadataCache,
    stats: &[u8],
    start: CaptureStart<'_>,
) -> Result<Capture> {
    let started = Instant::now();
    let CaptureStart {
        trigger,
        clock,
        capture_index,
        previous_capture_index,
    } = start;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    format::write_file_header(&mut file)?;
    storage.replace_file(file)?;
    storage.request_directory_sync(directory)?;
    // Bootstrap and first trigger share one submission, including when the
    // staging budget permits only one block.
    let index = storage
        .free_index()
        .context("initial capture requires a staging block")?;
    let block = storage.buffer_mut(index);
    ensure!(
        block.len() >= metadata.bytes.len(),
        "metadata cache exceeds staging block"
    );
    #[derive(Serialize)]
    struct CaptureChain {
        schema_version: u32,
        capture_index: Decimal,
        previous_capture_index: Option<Decimal>,
    }
    #[derive(Serialize)]
    struct CaptureManifest<'a> {
        #[serde(flatten)]
        session: &'a serde_json::Value,
        capture_chain: CaptureChain,
    }
    let manifest = CaptureManifest {
        session: &metadata.manifest,
        capture_chain: CaptureChain {
            schema_version: 1,
            capture_index: Decimal(capture_index),
            previous_capture_index: previous_capture_index.map(Decimal),
        },
    };
    // Borrow the cached session fields and encode directly into staging; only
    // the capture-chain identifiers change, so no deep clone or temporary Vec.
    let mut writer = std::io::Cursor::new(&mut block[HEADER_LEN..]);
    serde_json::to_writer(&mut writer, &manifest)?;
    let length = writer.position() as usize;
    let mut used = format::seal(
        block,
        RecordHeader::new(RecordKind::Metadata, 1, 0),
        length,
        length,
        0,
    )?;
    let remaining = &metadata.bytes[metadata.manifest_record_len..];
    ensure!(
        block.len() - used >= remaining.len(),
        "metadata cache exceeds staging block"
    );
    block[used..used + remaining.len()].copy_from_slice(remaining);
    used += remaining.len();
    append_clock(block, &mut used, metadata.next_sequence, clock)?;
    append_payload(
        block,
        &mut used,
        RecordHeader::new(
            RecordKind::Stats,
            metadata.next_sequence + 1,
            trigger.timestamp_ns,
        ),
        stats,
    )?;
    append_trigger(block, &mut used, metadata.next_sequence + 2, trigger)?;
    storage.submit(index, used)?;
    Ok(Capture {
        path,
        started,
        triggers: 1,
        first_trigger_ns: trigger.timestamp_ns,
        capture_index,
        endpoint: trigger.end_timestamp_ns,
        record_sequence: metadata.next_sequence + 3,
        clock_history_written: 0,
        phase: CapturePhase::Draining,
        snapshot_ns: 0,
        snapshot_banks: 1,
        first_bank_ready_ns: 0,
        first_drain_ns: None,
        post_polling_logged: false,
        next_occupancy_ns: 0,
        next_cpu: 0,
    })
}

// Limit each admission batch so continuous trigger traffic cannot starve swaps.
const TRIGGER_BATCH_LIMIT: usize = 16;

fn bank_drained(cpus: &[CpuCaptureState]) -> bool {
    cpus.iter().all(|cpu| cpu.completion.is_some())
}

fn validate_staging_budget(bytes: usize) -> Result<usize> {
    ensure!(
        bytes > 0 && bytes <= isize::MAX as usize,
        "staging budget must be positive and fit the address space"
    );
    Ok(bytes)
}

struct IdleReportSchedule {
    period: Duration,
    last: Instant,
    was_capturing: bool,
}

impl IdleReportSchedule {
    fn due(&mut self, now: Instant, capturing: bool) -> bool {
        // Start a fresh period after capture; never replay reports missed then.
        if capturing || self.was_capturing {
            self.last = now;
            self.was_capturing = capturing;
            return false;
        }
        if now.saturating_duration_since(self.last) >= self.period {
            self.last = now;
            return true;
        }
        false
    }
}

fn staging_layout(budget: usize, block_size: usize) -> Result<(usize, usize)> {
    ensure!(
        block_size > 0 && block_size <= u32::MAX as usize,
        "invalid staging block size"
    );
    let count = (budget / block_size).min(crate::storage::MAX_BLOCKS);
    ensure!(count > 0, "staging budget is too small: need at least {block_size}B ({}KiB rounded up) for one block; increase --staging", block_size.div_ceil(1024));
    // Division guarantees this product cannot exceed the requested budget.
    Ok((count, count * block_size))
}

fn event_timeout(
    now: Instant,
    clock_deadline: Instant,
    idle: Option<&IdleReportSchedule>,
) -> Duration {
    let mut timeout = clock_deadline.saturating_duration_since(now);
    if let Some(schedule) = idle {
        timeout = timeout.min(
            schedule
                .period
                .saturating_sub(now.duration_since(schedule.last)),
        );
    }
    timeout
}

/// Result of one recorder turn. Progress means call step again after servicing
/// other application work. Pending means wait for readiness. Finished means
/// accepted captures are synced and tracing has been torn down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Progress,
    Pending,
    Finished,
}

/// Recorder driven by the caller's event loop. Owns the supplied trigger source
/// and creates no thread or signal handler.
/// Request shutdown and keep stepping until Finished to complete captures.
/// Dropping an active recorder aborts capture and attempts cleanup. After an
/// error, drop it; further steps are not supported.
pub struct Recorder<S: TriggerSource> {
    readiness: Readiness,
    source: S,
    dir: File,
    trace: TraceSession,
    metadata: MetadataCache,
    storage: Storage,
    raw: Vec<u8>,
    accounting_scratch: Vec<u8>,
    compressor: Option<crate::compression::Compressor>,
    cpu_states: Vec<CpuCaptureState>,
    history_trim: Option<HistoryTrim>,
    next_trigger: u64,
    next_capture_index: u64,
    previous_capture_index: Option<u64>,
    active: Option<Capture>,
    exhaustion: u64,
    clock_monitor: ClockMonitor,
    idle_report: Option<IdleReportSchedule>,
    session_words: String,
    session_output: PathBuf,
    shutdown: Shutdown,
    finished: bool,
    failed: bool,
    stop_observed: bool,
}
impl<S: TriggerSource> Recorder<S> {
    /// Performs startup and emits Ready only after initialization succeeds.
    /// Takes ownership of the supplied trigger source and its readiness lifetime.
    pub fn new(
        config: Config,
        shutdown: Shutdown,
        source: S,
        mut emit: impl FnMut(StatusEvent),
    ) -> Result<Self> {
        let started = std::time::SystemTime::now();
        #[cfg(not(feature = "lz4"))]
        ensure!(
            config.compression.is_none(),
            "this recorder was built without LZ4 support; rebuild with --features lz4"
        );
        ensure!(
            config.trace.trace_clock == "mono",
            "v1 daemon requires mono trace clock"
        );
        ensure!(
            config.compression != Some(0),
            "compression reset interval must be positive"
        );
        let staging_budget = validate_staging_budget(config.staging_bytes)?;
        ensure!(config.sync_batch_bytes > 0, "sync batch must be positive");
        ensure!(
            config.idle_report.is_none_or(|period| !period.is_zero()),
            "idle report period must be positive"
        );
        let output_metadata = fs::metadata(&config.output).with_context(|| {
            format!(
                "output directory {} must already exist; provision storage before starting Retroft",
                config.output.display()
            )
        })?;
        ensure!(
            output_metadata.is_dir(),
            "output path {} is not a directory",
            config.output.display()
        );
        // Reserve a session directory exclusively; retry the word tuple on collision.
        let (session_id, session_words, session_key, session_output) = loop {
            let (id, words, key) = new_session_id(started)?;
            let path = config.output.join(&id);
            match fs::create_dir(&path) {
                Ok(()) => break (id, words, key, path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("create session directory"),
            }
        };
        // Persist the new session directory entry before reporting readiness.
        File::open(&config.output)?.sync_all()?;
        let dir = File::open(&session_output)?;
        let trace = TraceSession::start(&config.trace)?;
        let mut manifest = serde_json::to_value(trace.metadata())?;
        manifest["snapshot_writer_barrier"] = "membarrier_global".into();
        manifest["session_id"] = session_id.clone().into();
        manifest["session_key"] = session_key.into();
        manifest["recorder_version"] = env!("CARGO_PKG_VERSION").into();
        manifest["sync_batch_bytes"] = config.sync_batch_bytes.to_string().into();
        let history_probe_bytes = add_history_manifest(
            &mut manifest,
            config.history_margin,
            trace.cpu_ids().len(),
            trace.subbuffer_size(),
        )?;
        manifest["compression"] = if config.compression.is_some() {
            "lz4"
        } else {
            "none"
        }
        .into();
        if let Some(reset_bytes) = config.compression {
            manifest["compression_reset_bytes"] = reset_bytes.to_string().into();
        }
        let mut metadata = vec![(RecordKind::Metadata, serde_json::to_vec(&manifest)?)];
        for f in &trace.metadata().files {
            metadata.push((
                RecordKind::MetadataFile,
                format::metadata_file_payload(&f.path, &f.bytes)?,
            ));
        }
        let metadata = MetadataCache::new(&metadata)?;
        let page_size = trace.subbuffer_size();
        // Stats contain both named counters and the original tracefs text; reserve
        // per-CPU accounting/boundary capacity before allocating the reusable pool.
        let accounting_capacity = trace
            .cpu_ids()
            .len()
            .checked_mul(2048)
            .and_then(|n| n.checked_add(4096))
            .context("per-CPU accounting capacity overflow")?;
        let payload_size = crate::compression::compress_bound(page_size)
            .max(accounting_capacity)
            .max(64 * 1024);
        ensure!(
            payload_size <= format::MAX_PAYLOAD,
            "metadata/subbuffer exceeds maximum record size"
        );
        let initial_size = metadata
            .bytes
            .len()
            .checked_add(256)
            .and_then(|n| n.checked_add(HEADER_LEN + payload_size))
            .and_then(|n| n.checked_add(HEADER_LEN + TRIGGER_RESERVE))
            .and_then(|n| n.checked_add(HEADER_LEN + SNAPSHOT_RESERVE))
            .context("initial capture size overflow")?;
        let block_size = (HEADER_LEN + payload_size).max(initial_size);
        let (block_count, pool_size) = staging_layout(staging_budget, block_size)?;
        let storage = Storage::new(
            File::open("/dev/null")?,
            block_size,
            block_count,
            config.sync_batch_bytes,
        )?;
        let mut raw = vec![0u8; page_size];
        for offset in (0..page_size).step_by(4096) {
            unsafe {
                raw.as_mut_ptr().add(offset).write_volatile(0);
            }
        }
        let mut accounting_scratch = vec![0u8; payload_size];
        for offset in (0..payload_size).step_by(4096) {
            unsafe {
                accounting_scratch
                    .as_mut_ptr()
                    .add(offset)
                    .write_volatile(0);
            }
        }
        let compressor = config
            .compression
            .map(|reset_bytes| crate::compression::Compressor::new(reset_bytes, page_size))
            .transpose()?;
        let compression_status = match compressor.as_ref() {
            Some(compressor) => format!(
                "compression=lz4 compression_workspace={}B",
                compressor.workspace_bytes()
            ),
            None => "compression=off".into(),
        };
        let cpus = trace.cpu_ids().to_vec();
        let mut cpu_states: Vec<_> = cpus.iter().copied().map(CpuCaptureState::new).collect();
        let history_trim = config
            .history_margin
            .map(|margin| u64::try_from(margin.as_nanos()).context("history margin too large"))
            .transpose()?
            .map(|margin| HistoryTrim::new(cpus.len(), page_size, margin));
        for cpu in &mut cpu_states {
            cpu.history_discard = history_trim
                .as_ref()
                .map(|trim| HistoryDiscard::new(trim.margin_ns));
        }
        let next_trigger = 1u64;
        let next_capture_index = 1u64;
        let previous_capture_index = None;
        let active: Option<Capture> = None;
        let exhaustion = 0u64;
        let clock_monitor = ClockMonitor::new(crate::clock::sample()?, Instant::now());
        let idle_report = config.idle_report.map(|period| IdleReportSchedule {
            period,
            last: Instant::now(),
            was_capturing: false,
        });
        let readiness = Readiness::new(
            storage.completion_fd(),
            source.as_fd().as_raw_fd(),
            shutdown.fd(),
        )?;
        emit(StatusEvent::Ready { session_id: session_id.clone(), output: session_output.clone(), cpu_count: cpus.len(), summary: format!("ready: backend={} cpus={} kernel_buffers={} staging={} staging_budget={} staging_blocks={} staging_block_bytes={} staging_unused={} raw_scratch={} history_probe={} accounting_scratch={} cached_metadata={} {} session={}",trace.backend_name(),cpus.len(),trace.metadata().kernel_buffer_bytes,pool_size,staging_budget,block_count,block_size,staging_budget-pool_size,page_size,history_probe_bytes,payload_size,metadata.bytes.len(),compression_status,session_id) });
        for note in &trace.metadata().diagnostics {
            emit(StatusEvent::Diagnostic {
                message: format!("tracefs: {note}"),
            });
        }
        Ok(Self {
            readiness,
            source,
            dir,
            trace,
            metadata,
            storage,
            raw,
            accounting_scratch,
            compressor,
            cpu_states,
            history_trim,
            next_trigger,
            next_capture_index,
            previous_capture_index,
            active,
            exhaustion,
            clock_monitor,
            idle_report,
            session_words,
            session_output,
            shutdown,
            finished: false,
            failed: false,
            stop_observed: false,
        })
    }

    /// Does not sleep waiting for input or storage. Work is bounded to one raw
    /// sub-buffer and a small trigger batch, plus completion/state bookkeeping.
    /// Snapshot swaps, membarrier, filesystem setup and kernel calls can still
    /// block: this is cooperative scheduling, not a hard latency guarantee.
    pub fn step(&mut self, mut emit: impl FnMut(StatusEvent)) -> Result<Step> {
        ensure!(
            !self.failed,
            "recorder failed; drop it rather than retrying step"
        );
        if self.finished {
            return Ok(Step::Finished);
        }
        let result = self.advance(&mut emit).and_then(|step| {
            if step != Step::Finished {
                self.update_readiness(step == Step::Progress)?;
            }
            Ok(step)
        });
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn advance(&mut self, emit: &mut impl FnMut(StatusEvent)) -> Result<Step> {
        self.stop_observed = self.shutdown.is_requested();
        self.storage.reap()?;
        if let Some(message) = self.storage.take_warning() {
            emit(StatusEvent::Diagnostic { message });
        }
        let now = Instant::now();
        if self.clock_monitor.due(now) {
            let previous = self.clock_monitor.history().len();
            self.clock_monitor.refresh(now)?;
            if self.clock_monitor.history().len() != previous {
                emit(StatusEvent::Diagnostic {
                    message: format!(
                        "clock: resume detected; retained_snapshots={} history_bytes={}",
                        self.clock_monitor.history().len(),
                        self.clock_monitor.history_bytes()
                    ),
                });
            }
        }
        let mut progressed = false;
        progressed |= self.drain_page(emit)?;
        if let Some(cap) = &mut self.active {
            if cap.first_drain_ns.is_none() && bank_drained(&self.cpu_states) {
                cap.first_drain_ns = Some(monotonic_ns()?.saturating_sub(cap.first_bank_ready_ns));
                // Trimming only touches the first bank; totals are final now.
                if let Some(trim) = &self.history_trim {
                    let pages: u64 = self
                        .cpu_states
                        .iter()
                        .filter_map(|cpu| cpu.history_discard.as_ref())
                        .map(|d| d.pages)
                        .sum();
                    let bytes: u64 = self
                        .cpu_states
                        .iter()
                        .filter_map(|cpu| cpu.history_discard.as_ref())
                        .map(|d| d.raw_bytes.0)
                        .sum();
                    emit(StatusEvent::Diagnostic { message: format!(
                                "history trim: cutoff_before_trigger={} margin={:.3}s pages={} discarded={:.2} MiB",
                                trim.cutoff.map_or_else(
                                    || "none".to_owned(),
                                    |ns| format!("{:.3}s", (i128::from(cap.first_trigger_ns) - i128::from(ns)) as f64 / 1e9)
                                ),
                                trim.margin_ns as f64 / 1e9,
                                pages,
                                bytes as f64 / (1024.0 * 1024.0)
                            ) });
                }
            }
        }
        progressed |= self.accept_triggers(emit)?;
        progressed |= self.finish_or_swap(emit)?;
        if self.shutdown.is_requested() && self.active.is_none() {
            self.trace.finish()?;
            self.finished = true;
            return Ok(Step::Finished);
        }
        if !self.shutdown.is_requested()
            && self
                .idle_report
                .as_mut()
                .is_some_and(|schedule| schedule.due(Instant::now(), self.active.is_some()))
        {
            let result = self
                .trace
                .idle_status()
                .map(|observations| emit(StatusEvent::Idle { cpus: observations }));
            // A slow stats read or log destination must not cause immediate
            // back-to-back reports once this one finishes.
            if let Some(schedule) = &mut self.idle_report {
                schedule.last = Instant::now();
            }
            if let Err(error) = result {
                emit(StatusEvent::Diagnostic {
                    message: format!("idle report unavailable: {error:#}"),
                });
            }
        }
        Ok(if progressed {
            Step::Progress
        } else {
            Step::Pending
        })
    }

    fn drain_page(&mut self, emit: &mut impl FnMut(StatusEvent)) -> Result<bool> {
        let Some(cap) = &mut self.active else {
            return Ok(false);
        };
        if self.shutdown.is_requested() {
            cap.endpoint = cap.endpoint.min(monotonic_ns()?);
        }
        if clock_pending(cap, self.clock_monitor.history()) {
            return stage_clock_history(&mut self.storage, cap, self.clock_monitor.history());
        }
        if cap.phase != CapturePhase::Draining {
            return Ok(false);
        }
        let Some(i) = self.storage.free_index() else {
            self.exhaustion = self.exhaustion.saturating_add(1);
            return Ok(false);
        };
        // Frozen CPUs cannot overwrite each other; read one page in round-robin order.
        let count = self.cpu_states.len();
        let pos = (0..count)
            .map(|offset| (cap.next_cpu + offset) % count)
            .find(|&pos| {
                self.cpu_states[pos].completion.is_none()
                    && self.history_trim.as_ref().is_none_or(|trim| {
                        cap.snapshot_banks != 1 || !trim.probing || !trim.cpus[pos].probed
                    })
            });
        let Some(pos) = pos else {
            return Ok(false);
        };
        cap.next_cpu = (pos + 1) % count;
        let mut trim = self
            .history_trim
            .as_mut()
            .filter(|_| cap.snapshot_banks == 1);
        let info = match trim
            .as_mut()
            .and_then(|trim| trim.replay(pos, &mut self.raw))
        {
            Some(info) => Some(info),
            None => self.trace.read(self.cpu_states[pos].cpu, &mut self.raw)?,
        };
        if let Some(trim) = trim {
            if trim.probing
                || (trim.cutoff.is_some()
                    && !trim.cpus[pos].prefix_finished
                    && trim.cpus[pos].first_event.is_some())
            {
                let bounds = info
                    .map(|info| self.trace.event_bounds(&self.raw[..info.len], info))
                    .transpose()?
                    .flatten();
                if trim.probing {
                    let held = trim.probe(
                        pos,
                        info,
                        bounds,
                        &self.raw,
                        self.cpu_states[pos].history_discard.as_mut().unwrap(),
                    );
                    trim.finish_probe(cap.snapshot_ns, &mut self.cpu_states);
                    if held {
                        return Ok(true);
                    }
                } else if trim.discards(pos, cap.snapshot_banks, bounds) {
                    self.cpu_states[pos]
                        .history_discard
                        .as_mut()
                        .unwrap()
                        .discarded(info.unwrap());
                    return Ok(true);
                }
            }
        }
        let Some(info) = info else {
            self.cpu_states[pos].completion = Some(Completion::SnapshotDrained);
            return Ok(true);
        };
        // Initial bank wraps are ordinary prehistory eviction.
        // A later bank's loss means the live bank filled while we drained.
        if cap.snapshot_banks > 1 && info.has_lost_events && !self.cpu_states[pos].overrun_warned {
            emit(StatusEvent::Overrun {
                cpu: self.cpu_states[pos].cpu,
                path: cap.path.clone(),
            });
            self.cpu_states[pos].overrun_warned = true;
        }
        let block = self.storage.buffer_mut(i);
        let bytes = &self.raw[..info.len];
        let (len, codec, reset) = if let Some(c) = &mut self.compressor {
            let (n, reset) = c.compress(bytes, &mut block[HEADER_LEN..])?;
            if n < bytes.len() {
                (n, format::CODEC_LZ4, reset)
            } else {
                block[HEADER_LEN..HEADER_LEN + bytes.len()].copy_from_slice(bytes);
                (bytes.len(), format::CODEC_RAW, reset)
            }
        } else {
            block[HEADER_LEN..HEADER_LEN + bytes.len()].copy_from_slice(bytes);
            (bytes.len(), format::CODEC_RAW, false)
        };
        let mut h = RecordHeader::new(
            RecordKind::Trace,
            cap.record_sequence,
            info.base_timestamp_ns,
        );
        h.flags = if reset { format::HISTORY_RESET } else { 0 };
        h.cpu = self.cpu_states[pos].cpu;
        h.committed_bytes = u32::try_from(info.committed_bytes)?;
        h.skip_bytes = u32::try_from(info.skip_bytes)?;
        let n = format::seal(block, h, len, bytes.len(), codec)?;
        self.storage.submit(i, n)?;
        cap.record_sequence += 1;
        self.cpu_states[pos]
            .first_page_timestamp_ns
            .get_or_insert(Decimal(info.base_timestamp_ns));
        self.cpu_states[pos].last_page_timestamp_ns = Some(Decimal(info.base_timestamp_ns));
        Ok(true)
    }

    fn accept_triggers(&mut self, emit: &mut impl FnMut(StatusEvent)) -> Result<bool> {
        let mut accepted = false;
        for _ in 0..TRIGGER_BATCH_LIMIT {
            if !self.accepts_triggers() {
                break;
            }
            let Some(trigger) = receive_trigger(&mut self.source, self.next_trigger)? else {
                break;
            };
            let next = self
                .next_trigger
                .checked_add(1)
                .context("trigger sequence exhausted")?;
            if let Some(cap) = &mut self.active {
                let index = self
                    .storage
                    .free_index()
                    .context("trigger requires a staging block")?;
                let mut used = 0;
                append_trigger(
                    self.storage.buffer_mut(index),
                    &mut used,
                    cap.record_sequence,
                    &trigger,
                )?;
                self.storage.submit(index, used)?;
                cap.record_sequence = cap
                    .record_sequence
                    .checked_add(1)
                    .context("record sequence exhausted")?;
                let extension_ns = trigger.end_timestamp_ns.saturating_sub(cap.endpoint);
                cap.merge(trigger.end_timestamp_ns);
                emit(StatusEvent::TriggerAdded {
                    path: cap.path.clone(),
                    capture_index: cap.capture_index,
                    trigger: trigger.status(cap.endpoint),
                    extension_ns,
                });
            } else {
                // Prepare the file while the live ring can still overwrite old
                // history; filesystem latency must not spend post-swap headroom.
                let clock = ClockSnapshot {
                    sample: self.clock_monitor.refresh(Instant::now())?,
                    reason: Reason::FileStart,
                };
                let capture_time = utc_filename_time(std::time::SystemTime::now());
                let path = self.session_output.join(format!(
                    "{capture_time}_{}_{}.retroft",
                    self.session_words, self.next_capture_index
                ));
                let stats_len = accounting(
                    &mut self.accounting_scratch,
                    "start",
                    self.trace.stats_for_serialization(),
                    None,
                    None,
                )?;
                self.active = Some(start_capture(
                    &mut self.storage,
                    path,
                    &self.dir,
                    &self.metadata,
                    &self.accounting_scratch[..stats_len],
                    CaptureStart {
                        trigger: &trigger,
                        clock,
                        capture_index: self.next_capture_index,
                        previous_capture_index: self.previous_capture_index,
                    },
                )?);
                if let Some(cap) = &self.active {
                    emit(StatusEvent::CaptureStarted {
                        path: cap.path.clone(),
                        capture_index: cap.capture_index,
                        trigger: trigger.status(cap.endpoint),
                    });
                }
                self.next_capture_index = self
                    .next_capture_index
                    .checked_add(1)
                    .context("capture index exhausted")?;
                let snapshot_ns = monotonic_ns()?;
                self.trace.swap_snapshot()?;
                let cap = self.active.as_mut().unwrap();
                cap.first_bank_ready_ns = monotonic_ns()?;
                cap.snapshot_ns = snapshot_ns;
                cap.next_occupancy_ns = snapshot_ns.saturating_add(OCCUPANCY_CHECK_NS);
            }
            self.next_trigger = next;
            accepted = true;
        }
        Ok(accepted)
    }

    fn accepts_triggers(&self) -> bool {
        !self.shutdown.is_requested()
            && self.storage.free_index().is_some()
            && self.active.as_ref().is_none_or(|cap| {
                !cap.phase.sealed()
                    && bank_drained(&self.cpu_states)
                    && !clock_pending(cap, self.clock_monitor.history())
            })
    }

    fn finish_or_swap(&mut self, emit: &mut impl FnMut(StatusEvent)) -> Result<bool> {
        let Some(cap) = &mut self.active else {
            return Ok(false);
        };
        if clock_pending(cap, self.clock_monitor.history()) {
            return Ok(false);
        }
        let mut progressed = false;
        // Draining -> Closing (End queued) -> Syncing (final sync submitted).
        match cap.phase {
            CapturePhase::Draining => {
                if self.cpu_states.iter().all(|cpu| cpu.completion.is_some()) {
                    if cap.snapshot_ns >= cap.endpoint {
                        // Append accounting and End before the final file sync.
                        // Batch-size syncs may already have persisted earlier data.
                        let Some(index) = self.storage.free_index() else {
                            return Ok(false);
                        };
                        let len = accounting(
                            &mut self.accounting_scratch,
                            "end",
                            self.trace.stats_for_serialization(),
                            Some(self.exhaustion),
                            Some(&self.cpu_states),
                        )?;
                        let next = cap
                            .record_sequence
                            .checked_add(2)
                            .context("record sequence exhausted")?;
                        let now = monotonic_ns()?;
                        let block = self.storage.buffer_mut(index);
                        let mut used = 0;
                        append_payload(
                            block,
                            &mut used,
                            RecordHeader::new(RecordKind::Stats, cap.record_sequence, now),
                            &self.accounting_scratch[..len],
                        )?;
                        #[derive(Serialize)]
                        struct End {
                            schema_version: u32,
                            completed: bool,
                            capture_index: Decimal,
                            end_timestamp_ns: Decimal,
                            shutdown: bool,
                        }
                        let end = End {
                            schema_version: 1,
                            completed: true,
                            capture_index: Decimal(cap.capture_index),
                            end_timestamp_ns: Decimal(cap.endpoint),
                            shutdown: self.shutdown.is_requested(),
                        };
                        let mut bytes = [0u8; 256];
                        let mut writer = std::io::Cursor::new(&mut bytes[..]);
                        serde_json::to_writer(&mut writer, &end)?;
                        let length = writer.position() as usize;
                        append_payload(
                            block,
                            &mut used,
                            RecordHeader::new(RecordKind::End, cap.record_sequence + 1, now),
                            &bytes[..length],
                        )?;
                        self.storage.submit(index, used)?;
                        cap.record_sequence = next;
                        cap.phase = CapturePhase::Closing;
                        progressed = true;
                    } else if cap
                        .swap_due(monotonic_ns()?, || self.trace.active_bank_needs_drain())?
                    {
                        // Keep the pre-swap lower bound for endpoint coverage;
                        // swap_snapshot also waits for outstanding writers.
                        let snapshot_ns = monotonic_ns()?;
                        self.trace.swap_snapshot()?;
                        cap.snapshot_ns = snapshot_ns;
                        cap.snapshot_banks += 1;
                        cap.next_occupancy_ns = snapshot_ns.saturating_add(OCCUPANCY_CHECK_NS);
                        progressed = true;
                        for cpu in &mut self.cpu_states {
                            cpu.completion = None;
                        }
                    } else if !cap.post_polling_logged {
                        // Log only after trigger admission and the occupancy
                        // check establish that we can wait instead of draining.
                        emit(StatusEvent::Diagnostic { message: format!(
                                    "post-trigger polling: first_drain={:.3}s post_remaining={:.3}s interval=100ms",
                                    cap.first_drain_ns.unwrap_or(0) as f64 / 1e9,
                                    cap.endpoint.saturating_sub(monotonic_ns()?) as f64 / 1e9,
                                ) });
                        cap.post_polling_logged = true;
                    }
                }
            }
            CapturePhase::Closing if self.storage.idle() => {
                self.storage.request_sync()?;
                cap.phase = CapturePhase::Syncing;
                progressed = true;
            }
            CapturePhase::Syncing if self.storage.idle() => {
                emit(StatusEvent::CaptureCompleted {
                    path: cap.path.clone(),
                    capture_index: cap.capture_index,
                    bytes: self.storage.logical_end(),
                    elapsed_ns: cap.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                    triggers: cap.triggers,
                    snapshot_banks: cap.snapshot_banks,
                });
                self.previous_capture_index = Some(cap.capture_index);
                self.active = None;
                // Leave reusable state ready for the next trigger. Keep this
                // after completion accounting/logs consume the old values.
                if let Some(c) = &mut self.compressor {
                    c.reset();
                }
                if let Some(trim) = &mut self.history_trim {
                    trim.reset();
                }
                for cpu in &mut self.cpu_states {
                    *cpu = CpuCaptureState::new(cpu.cpu);
                    cpu.history_discard = self
                        .history_trim
                        .as_ref()
                        .map(|trim| HistoryDiscard::new(trim.margin_ns));
                }
                self.exhaustion = 0;
                progressed = true;
            }
            _ => {} // Still waiting for a write or sync completion.
        }
        Ok(progressed)
    }

    fn update_readiness(&mut self, runnable: bool) -> Result<()> {
        let admit = self.accepts_triggers();
        let runnable = runnable || self.shutdown.is_requested() != self.stop_observed;
        let deadline = if runnable {
            None
        } else {
            let now = Instant::now();
            let at = now
                + event_timeout(
                    now,
                    self.clock_monitor.next_sample(),
                    if self.active.is_none() {
                        self.idle_report.as_ref()
                    } else {
                        None
                    },
                );
            let mut deadline = Deadline::Instant(at);
            if let Some(cap) = self.active.as_ref().filter(|cap| {
                cap.phase == CapturePhase::Draining
                    && bank_drained(&self.cpu_states)
                    && !clock_pending(cap, self.clock_monitor.history())
            }) {
                let ns = cap.endpoint.min(cap.next_occupancy_ns);
                if Duration::from_nanos(ns.saturating_sub(monotonic_ns()?))
                    < at.saturating_duration_since(Instant::now())
                {
                    deadline = Deadline::Monotonic(ns);
                }
            }
            Some(deadline)
        };
        self.readiness
            .update(runnable, admit, !self.stop_observed, deadline)
    }
}

/// Readable whenever step can make progress or a timer needs servicing. Register
/// once in an outer level-triggered poll/epoll loop. Do not read or epoll_wait on
/// this descriptor; step owns the child epoll and all of its notifications.
impl<S: TriggerSource> AsFd for Recorder<S> {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.readiness.as_fd()
    }
}

/// Blocking loop over Recorder::step and its readiness descriptor. Suitable for
/// a main thread or dedicated worker. The caller supplies signal handling,
/// systemd notifications, and status reporting.
pub fn run(
    config: Config,
    shutdown: &Shutdown,
    source: impl TriggerSource,
    mut emit: impl FnMut(StatusEvent),
) -> Result<()> {
    let mut recorder = Recorder::new(config, shutdown.clone(), source, &mut emit)?;
    loop {
        match recorder.step(&mut emit)? {
            Step::Finished => return Ok(()),
            Step::Progress => continue,
            Step::Pending => {}
        }
        let mut fd = libc::pollfd {
            fd: recorder.as_fd().as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: recorder owns the descriptor; fd is initialized and live.
        if unsafe { libc::poll(&mut fd, 1, -1) } < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error).context("wait for recorder");
            }
        }
        ensure!(
            fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) == 0,
            "recorder readiness failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn event_deadlines_follow_clock_and_reports() {
        let now = Instant::now();
        let clock = ClockMonitor::new(
            crate::clock::ClockSample {
                monotonic_ns: 0,
                boottime_ns: 0,
                realtime_ns: 0,
                uncertainty_ns: 0,
            },
            now,
        );
        assert_eq!(
            event_timeout(now, clock.next_sample(), None),
            Duration::from_secs(1)
        );
        let report = IdleReportSchedule {
            period: Duration::from_secs(5),
            last: now,
            was_capturing: false,
        };
        assert_eq!(
            event_timeout(now, clock.next_sample(), Some(&report)),
            Duration::from_secs(1)
        );
        let report = IdleReportSchedule {
            period: Duration::from_millis(20),
            ..report
        };
        assert_eq!(
            event_timeout(now, clock.next_sample(), Some(&report),),
            Duration::from_millis(20)
        );
    }
    #[test]
    fn typed_sources_share_validation_and_sequence_assignment() -> Result<()> {
        let (sender, mut input) = crate::trigger::channel(4)?;
        sender.try_send(TriggerRequest::new(String::new(), "invalid".into(), None))?;
        sender.try_send(TriggerRequest::new("bus".into(), "valid".into(), None))?;
        assert!(receive_trigger(&mut input, 9)?.is_none());
        let trigger = receive_trigger(&mut input, 9)?.unwrap();
        assert_eq!(trigger.sequence, 9);
        assert_eq!(trigger.end_timestamp_ns, trigger.timestamp_ns);
        assert!(trigger.peer.is_none());
        let status = trigger.status(trigger.end_timestamp_ns);
        assert_eq!(status.source.as_deref(), Some("bus"));
        assert_eq!(status.description, "valid");
        Ok(())
    }
    #[test]
    fn session_ids_have_basic_utc_timestamp_and_three_words() -> Result<()> {
        let started = std::time::SystemTime::now();
        let (first, session_words, key) = new_session_id(started)?;
        let (_, _, other_key) = new_session_id(started)?;
        assert_ne!(key, other_key);
        assert_eq!(key.len(), 32);
        assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(
            SESSION_WORDS
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            256
        );
        let (timestamp, suffix) = first.split_once("Z_").unwrap();
        assert_eq!(timestamp.len(), 15);
        assert_eq!(&timestamp[8..9], "T");
        let rfc3339 = format!(
            "{}-{}-{}T{}:{}:{}Z",
            &timestamp[..4],
            &timestamp[4..6],
            &timestamp[6..8],
            &timestamp[9..11],
            &timestamp[11..13],
            &timestamp[13..]
        );
        humantime::parse_rfc3339(&rfc3339)?;
        assert_eq!(suffix, session_words);
        assert_eq!(format!("{timestamp}Z"), utc_filename_time(started));
        let words: Vec<_> = suffix.split('-').collect();
        assert_eq!(words.len(), 3);
        assert!(words.iter().all(|word| SESSION_WORDS.contains(word)));
        Ok(())
    }

    #[test]
    fn idle_reports_skip_captures_and_do_not_catch_up_in_bursts() {
        let start = Instant::now();
        let mut schedule = IdleReportSchedule {
            period: Duration::from_secs(5),
            last: start,
            was_capturing: false,
        };
        assert!(!schedule.due(start + Duration::from_secs(4), false));
        assert!(schedule.due(start + Duration::from_secs(5), false));
        assert!(!schedule.due(start + Duration::from_secs(5), false));
        assert!(!schedule.due(start + Duration::from_secs(100), true));
        assert!(!schedule.due(start + Duration::from_secs(104), false));
        assert!(!schedule.due(start + Duration::from_secs(108), false));
        assert!(schedule.due(start + Duration::from_secs(109), false));
        assert!(schedule.due(start + Duration::from_secs(200), false));
        assert!(!schedule.due(start + Duration::from_secs(200), false));
        assert!(!schedule.due(start + Duration::from_secs(201), true));
        assert!(!schedule.due(start + Duration::from_secs(300), false));
        assert!(schedule.due(start + Duration::from_secs(305), false));
    }
    #[test]
    fn staging_budget_rounds_down_and_checks_limits() -> Result<()> {
        assert_eq!(staging_layout(1024, 300)?, (3, 900));
        assert_eq!(staging_layout(300, 300)?, (1, 300));
        assert!(staging_layout(1024, 1025)
            .unwrap_err()
            .to_string()
            .contains("at least 1025B (2KiB rounded up)"));
        assert_eq!(
            staging_layout(100_000, 1)?,
            (crate::storage::MAX_BLOCKS, crate::storage::MAX_BLOCKS)
        );
        assert_eq!(validate_staging_budget(8 * 1024 * 1024)?, 8 * 1024 * 1024);
        assert!(validate_staging_budget(0).is_err());
        assert!(validate_staging_budget(usize::MAX).is_err());
        assert!(validate_staging_budget(isize::MAX as usize + 1).is_err());
        Ok(())
    }
    #[test]
    fn trigger_protocol_and_exact_integers() -> Result<()> {
        let n = 9_007_199_254_740_999;
        let t = Trigger::parse(br#"{"source":"test","description":"test"}"#, n, 123)?;
        assert_eq!(t.end_timestamp_ns, 123);
        let mut buf = [0; 2048];
        let size = t.encode(&mut buf)?;
        let json: serde_json::Value = serde_json::from_slice(&buf[..size])?;
        assert_eq!(json["sequence"], n.to_string());
        assert_eq!(
            Trigger::parse(br#"{"source":"test","end_timestamp_ns":"122"}"#, 1, 123)?
                .end_timestamp_ns,
            122
        );
        Ok(())
    }
    #[test]
    fn trigger_source_bounds_and_encoding_reserve() -> Result<()> {
        let bytes = serde_json::to_vec(&serde_json::json!({"source": "x".repeat(MAX_SOURCE + 1)}))?;
        assert!(Trigger::parse(&bytes, 1, 0).is_err());
        let mut trigger = Trigger::parse(br#"{"source":"service","description":"event"}"#, 1, 0)?;
        assert_eq!(trigger.source.as_deref(), Some("service"));
        // Exercise the serializer's worst expansion independently of wire limits.
        trigger.source = Some("\0".repeat(MAX_SOURCE));
        trigger.description = "\0".repeat(MAX_DESCRIPTION);
        trigger.peer = Some(PeerIdentity {
            peer_pid: 123,
            peer_uid: 456,
            peer_gid: 789,
            peer_process_name: Some("\0".repeat(256)),
        });
        let mut output = vec![0; TRIGGER_RESERVE];
        let len = trigger.encode(&mut output)?;
        let json: serde_json::Value = serde_json::from_slice(&output[..len])?;
        assert_eq!(json["peer_pid"], 123);
        assert_eq!(json["source"].as_str().unwrap().len(), MAX_SOURCE);
        Ok(())
    }

    #[test]
    fn escaped_descriptions_and_invalid_requests() -> Result<()> {
        let trigger = Trigger::parse(
            br#"{"source":"test","description":"line\nquote\" slash\\ \ud83d\ude80"}"#,
            1,
            5,
        )?;
        assert_eq!(trigger.description, "line\nquote\" slash\\ 🚀");
        for bytes in [
            br#"{"ack":true,"ack":false}"#.as_slice(),
            br#"{"description":null,"description":"duplicate"}"#,
            br#"{"source":null,"source":"duplicate"}"#,
            br#"{"source":123}"#,
            br#"{"source":""}"#,
            br#"{"source":null}"#,
            br#"{}"#,
            br#"{"peer_pid":123}"#,
            br#"{"end_timestamp_ns":null,"end_timestamp_ns":"4"}"#,
            br#"{"ack":null}"#,
            br#"{"description":"\ud800"}"#,
            br#"{"description":"\udc00"}"#,
            br#"{"end_timestamp_ns":"+4"}"#,
            br#"{"end_timestamp_ns":"04"}"#,
            br#"{"end_timestamp_ns":""}"#,
            br#"{"end_timestamp_ns":4}"#,
            br#"{"end_timestamp_ns":"18446744073709551616"}"#,
            br#"{"unknown":1}"#,
            br#"{"ack":false,}"#,
            br#"{} {}"#,
        ] {
            assert!(Trigger::parse(bytes, 1, 5).is_err());
        }
        let mut bytes = [b'a'; MAX_MESSAGE];
        bytes[..16].copy_from_slice(b"{\"description\":\"");
        bytes[1041..1043].copy_from_slice(b"\"}");
        assert!(Trigger::parse(&bytes[..1043], 1, 5).is_err());
        assert!(Trigger::parse(&[b' '; MAX_MESSAGE + 1], 1, 5).is_err());
        assert_eq!(
            Trigger::parse(br#"{"source":"test","end_timestamp_ns":"0"}"#, 1, 5)?.end_timestamp_ns,
            0
        );
        assert!(Trigger::parse(
            br#"{"source":"test","description":null,"end_timestamp_ns":null}"#,
            1,
            5
        )?
        .description
        .is_empty());
        Ok(())
    }
    fn trim_test_states(count: usize, margin: u64) -> Vec<CpuCaptureState> {
        (0..count)
            .map(|cpu| {
                let mut state = CpuCaptureState::new(cpu as u32);
                state.history_discard = Some(HistoryDiscard::new(margin));
                state
            })
            .collect()
    }
    fn probe_page(lost: bool) -> crate::tracefs::ReadInfo {
        crate::tracefs::ReadInfo {
            len: 64,
            base_timestamp_ns: 0,
            committed_bytes: 48,
            skip_bytes: 0,
            lost_events: None,
            has_lost_events: lost,
        }
    }
    #[test]
    fn leading_probe_discards_retained_fragment_before_second_page_loss() {
        let mut trim = HistoryTrim::new(2, 64, 10);
        let mut states = trim_test_states(2, 10);
        let old = [1; 64];
        let current = [2; 64];
        assert!(trim.probe(
            0,
            Some(probe_page(false)),
            Some((100, 195)),
            &old,
            states[0].history_discard.as_mut().unwrap()
        ));
        assert!(!trim.cpus[0].probed);
        let mut raw = [0; 64];
        assert!(trim.replay(0, &mut raw).is_none());
        assert!(trim.probe(
            0,
            Some(probe_page(true)),
            Some((200, 250)),
            &current,
            states[0].history_discard.as_mut().unwrap()
        ));
        // A quiet CPU's much later first event must not move the cutoff.
        trim.probe(
            1,
            Some(probe_page(false)),
            Some((900, 950)),
            &old,
            states[1].history_discard.as_mut().unwrap(),
        );
        assert!(trim.probe(
            1,
            None,
            None,
            &[],
            states[1].history_discard.as_mut().unwrap()
        ));
        trim.finish_probe(1000, &mut states);
        assert_eq!(trim.cutoff, Some(190));
        let discarded = states[0].history_discard.as_ref().unwrap();
        assert_eq!(discarded.pages, 1); // Discard even though it crossed cutoff.
        assert_eq!(discarded.raw_bytes.0, 64);
        assert!(trim.replay(0, &mut raw).unwrap().has_lost_events);
        assert_eq!(raw, current);
        assert!(trim.replay(0, &mut raw).is_none());
        assert!(trim.replay(1, &mut raw).is_some());
        assert_eq!(raw, old);
        assert!(trim.replay(1, &mut raw).is_none());
    }
    #[test]
    fn leading_probe_preserves_two_pages_without_loss_and_resets() {
        let mut trim = HistoryTrim::new(1, 64, 0);
        let mut states = trim_test_states(1, 0);
        for (timestamp, byte) in [(100, 1), (900, 2)] {
            assert!(trim.probe(
                0,
                Some(probe_page(false)),
                Some((timestamp, timestamp)),
                &[byte; 64],
                states[0].history_discard.as_mut().unwrap()
            ));
        }
        trim.finish_probe(1000, &mut states);
        assert_eq!(trim.cutoff, None);
        let mut raw = [0; 64];
        for byte in [1, 2] {
            assert!(trim.replay(0, &mut raw).is_some());
            assert_eq!(raw, [byte; 64]);
        }
        assert!(trim.replay(0, &mut raw).is_none());
        trim.reset();
        assert_eq!(trim.cpus[0].seen, 0);
        assert!(!trim.probe(
            0,
            None,
            None,
            &[],
            states[0].history_discard.as_mut().unwrap()
        ));
        trim.finish_probe(1000, &mut states);
        assert_eq!(trim.cutoff, None);
    }
    #[test]
    fn leading_probe_handles_loss_on_control_page_before_second_event_page() {
        let mut trim = HistoryTrim::new(1, 64, 0);
        let mut states = trim_test_states(1, 0);
        let discarded = states[0].history_discard.as_mut().unwrap();
        trim.probe(
            0,
            Some(probe_page(false)),
            Some((10, 20)),
            &[1; 64],
            discarded,
        );
        assert!(!trim.probe(0, Some(probe_page(true)), None, &[0; 64], discarded));
        assert!(!trim.cpus[0].probed);
        trim.probe(
            0,
            Some(probe_page(false)),
            Some((100, 110)),
            &[2; 64],
            discarded,
        );
        trim.finish_probe(1000, &mut states);
        assert_eq!(trim.cutoff, Some(100));
        assert_eq!(states[0].history_discard.as_ref().unwrap().pages, 1);
    }
    #[test]
    fn leading_trim_uses_only_loss_backed_boundaries_and_keeps_sparse_history() {
        let mut trim = HistoryTrim::new(3, 64, 10);
        let mut states = trim_test_states(3, 10);
        for (cpu, first) in trim.cpus.iter_mut().zip([Some(100), Some(900), None]) {
            cpu.first_event = first;
        }
        for cpu in &mut trim.cpus {
            cpu.probed = true;
        }
        trim.finish_probe(1000, &mut states);
        // A recently drained bank or a quiet CPU's late first event proves no loss.
        assert_eq!(trim.cutoff, None);
        assert!(!trim.discards(0, 1, Some((100, 110))));
        trim.reset();
        for (cpu, first) in trim.cpus.iter_mut().zip([Some(100), Some(900), None]) {
            cpu.first_event = first;
        }
        trim.cpus[0].leading_loss = true;
        for cpu in &mut trim.cpus {
            cpu.probed = true;
        }
        trim.finish_probe(1000, &mut states);
        assert_eq!(trim.cutoff, Some(90));
        // Initial empty CPUs stay untrimmed even if a writer commits late.
        assert!(!trim.discards(2, 1, Some((1, 20))));
    }
    #[test]
    fn leading_trim_keeps_straddling_pages_and_never_trims_after_prefix_or_next_bank() {
        let mut trim = HistoryTrim::new(2, 64, 5);
        let mut states = trim_test_states(2, 5);
        for (cpu, first) in trim.cpus.iter_mut().zip([Some(10), Some(100)]) {
            cpu.first_event = first;
        }
        trim.cpus[1].leading_loss = true;
        for cpu in &mut trim.cpus {
            cpu.probed = true;
        }
        trim.finish_probe(90, &mut states);
        // Clamp the common boundary to the initial swap, then apply the margin.
        assert_eq!(trim.cutoff, Some(85));
        assert!(trim.discards(0, 1, Some((10, 84))));
        assert!(!trim.discards(0, 1, Some((80, 85))));
        // Subsequent timestamps going backwards do not restart prefix trimming.
        assert!(!trim.discards(0, 1, Some((20, 30))));
        assert!(!trim.discards(1, 2, Some((1, 2))));
        assert!(!trim.discards(1, 1, None));
        assert_eq!(trim.cutoff, Some(85));
    }
    #[test]
    fn leading_probe_waits_for_every_cpu_and_preserves_cache_across_extensions() {
        let mut trim = HistoryTrim::new(2, 64, 0);
        let mut states = trim_test_states(2, 0);
        trim.cpus[0].first_event = Some(100);
        trim.cpus[0].leading_loss = true; // May have arrived on a preceding control-only page.
        trim.cpus[0].probed = true;
        trim.cpus[0].pages[0][0] = 42;
        trim.finish_probe(200, &mut states);
        assert!(trim.probing);
        assert_eq!(trim.cutoff, None);
        trim.cpus[1].probed = true; // EOF CPU does not constrain the common boundary.
        trim.finish_probe(200, &mut states);
        assert!(!trim.probing);
        assert_eq!(trim.cutoff, Some(100));
        trim.cpus[1].first_event = Some(900); // Later arrival cannot move the fixed cutoff.
        trim.finish_probe(1000, &mut states);
        assert_eq!(trim.cutoff, Some(100));
        let allocation = trim.cpus[0].pages[0].as_ptr();
        trim.reset();
        assert_eq!(trim.cpus[0].pages[0].as_ptr(), allocation);
        assert!(trim.probing);
        assert_eq!(trim.cutoff, None);
    }
    #[test]
    fn history_manifest_records_policy_and_probe_memory_exactly() -> Result<()> {
        let mut manifest = serde_json::json!({});
        let margin_ns = (1u64 << 53) + 123;
        assert_eq!(
            add_history_manifest(
                &mut manifest,
                Some(Duration::from_nanos(margin_ns)),
                32,
                4096
            )?,
            262144
        );
        let serialized = serde_json::to_vec(&manifest)?;
        let decoded: serde_json::Value = serde_json::from_slice(&serialized)?;
        assert_eq!(decoded["history_margin_ns"], margin_ns.to_string());
        assert_eq!(decoded["history_probe_bytes"], "262144");
        assert_eq!(add_history_manifest(&mut manifest, None, 32, 4096)?, 0);
        assert!(manifest["history_margin_ns"].is_null());
        assert_eq!(manifest["history_probe_bytes"], "0");
        assert!(add_history_manifest(&mut manifest, Some(Duration::ZERO), usize::MAX, 2).is_err());
        Ok(())
    }

    #[test]
    fn history_discard_accounting_separates_exact_and_unknown_kernel_loss() -> Result<()> {
        let mut discard = HistoryDiscard::new(0);
        let info = crate::tracefs::ReadInfo {
            len: 64,
            base_timestamp_ns: 10,
            committed_bytes: 48,
            skip_bytes: 0,
            lost_events: Some(7),
            has_lost_events: true,
        };
        discard.discarded(info);
        discard.discarded(crate::tracefs::ReadInfo {
            lost_events: None,
            ..info
        });
        assert_eq!(discard.pages, 2);
        assert_eq!(discard.raw_bytes.0, 128);
        assert_eq!(discard.kernel_loss_pages, 2);
        assert_eq!(discard.known_lost_events.0, 7);
        assert_eq!(discard.unknown_loss_pages, 1);
        let json = serde_json::to_value(&discard)?;
        assert_eq!(json["known_lost_events"], "7");
        // Keep-all mode leaves the trim accounting absent.
        let cpu = serde_json::to_value(CpuCaptureState::new(0))?;
        assert!(cpu.get("history_discard").is_none());
        Ok(())
    }

    #[test]
    fn occupancy_pacing_waits_checks_and_honors_endpoint_and_extension() -> Result<()> {
        let mut cap = Capture {
            path: PathBuf::new(),
            started: Instant::now(),
            triggers: 1,
            first_trigger_ns: 0,
            capture_index: 1,
            endpoint: 1_000_000_000,
            record_sequence: 1,
            clock_history_written: 0,
            phase: CapturePhase::Draining,
            snapshot_ns: 0,
            snapshot_banks: 1,
            first_bank_ready_ns: 0,
            first_drain_ns: None,
            post_polling_logged: false,
            next_occupancy_ns: OCCUPANCY_CHECK_NS,
            next_cpu: 0,
        };
        assert!(!cap.swap_due(1, || panic!("checked before deadline"))?);
        assert!(!cap.swap_due(OCCUPANCY_CHECK_NS, || Ok(false))?);
        assert_eq!(cap.next_occupancy_ns, 2 * OCCUPANCY_CHECK_NS);
        assert!(!cap.swap_due(OCCUPANCY_CHECK_NS + 1, || panic!("busy polling"))?);
        assert!(cap.swap_due(2 * OCCUPANCY_CHECK_NS, || Ok(true))?);
        cap.next_occupancy_ns = 2_000_000_000;
        assert!(cap.swap_due(cap.endpoint, || panic!("endpoint must bypass occupancy"))?);
        cap.merge(3_000_000_000);
        assert!(!cap.swap_due(1_000_000_000, || panic!("extension should keep collecting"))?);
        assert!(cap
            .swap_due(2_000_000_000, || anyhow::bail!("stats unavailable"))
            .is_err());
        cap.endpoint = 0;
        assert!(cap.swap_due(1, || panic!("zero duration must not poll"))?);
        Ok(())
    }

    #[test]
    fn triggers_extend_draining_but_cannot_reopen_a_sealed_capture() {
        for phase in [
            CapturePhase::Draining,
            CapturePhase::Closing,
            CapturePhase::Syncing,
        ] {
            let mut cap = Capture {
                path: PathBuf::new(),
                started: Instant::now(),
                triggers: 1,
                first_trigger_ns: 0,
                capture_index: 1,
                endpoint: 10,
                record_sequence: 1,
                clock_history_written: 0,
                phase,
                snapshot_ns: 10,
                snapshot_banks: 1,
                first_bank_ready_ns: 0,
                first_drain_ns: None,
                post_polling_logged: false,
                next_occupancy_ns: 0,
                next_cpu: 0,
            };
            let mut cpus = [CpuCaptureState::new(0), CpuCaptureState::new(3)];
            assert!(!bank_drained(&cpus));
            cpus[0].completion = Some(Completion::SnapshotDrained);
            assert!(!bank_drained(&cpus));
            cpus[1].completion = Some(Completion::SnapshotDrained);
            assert!(bank_drained(&cpus));
            cpus[0].first_page_timestamp_ns = Some(Decimal(5));
            cpus[0].overrun_warned = true;
            cap.merge(20);
            if phase.sealed() {
                assert_eq!(cap.triggers, 1);
                assert_eq!(cap.endpoint, 10);
                assert_eq!(cap.phase, phase);
                assert!(cpus.iter().all(|cpu| cpu.completion.is_some()));
            } else {
                assert_eq!(cap.triggers, 2);
                assert_eq!(cap.endpoint, 20);
                assert_eq!(cap.phase, CapturePhase::Draining);
                // Extending requests another bank without rereading the frozen bank.
                assert!(cpus.iter().all(|cpu| cpu.completion.is_some()));
                // Extending a file keeps its existing bounds and warning state.
                assert_eq!(cpus[0].first_page_timestamp_ns.unwrap().0, 5);
                assert!(cpus[0].overrun_warned);
                cap.merge(19);
                assert_eq!(cap.triggers, 3);
                assert_eq!(cap.phase, CapturePhase::Draining);
                assert_eq!(cpus[0].completion, Some(Completion::SnapshotDrained));
            }
        }
    }

    #[test]
    fn accounting_uses_fixed_capacity_and_exact_decimal_strings() -> Result<()> {
        let mut bytes = [0u8; 1024];
        let cpus = [0u32, 1];
        let mut states = [CpuCaptureState::new(0), CpuCaptureState::new(1)];
        states[0].first_page_timestamp_ns = Some(Decimal(u64::MAX));
        states[0].last_page_timestamp_ns = Some(Decimal(u64::MAX));
        states[0].completion = Some(Completion::SnapshotDrained);
        states[1].completion = Some(Completion::SnapshotDrained);
        let count = accounting(&mut bytes, "end", cpus, Some(u64::MAX), Some(&states))?;
        let parsed: serde_json::Value = serde_json::from_slice(&bytes[..count])?;
        assert_eq!(
            parsed["staging_exhaustion_observations"],
            u64::MAX.to_string()
        );
        assert_eq!(
            parsed["cpu_boundaries"][0]["last_page_timestamp_ns"],
            u64::MAX.to_string()
        );
        assert_eq!(
            parsed["cpu_boundaries"][1]["completion"],
            "snapshot_drained"
        );
        assert!(accounting(&mut bytes[..8], "start", cpus, None, None).is_err());
        Ok(())
    }

    #[test]
    fn datagrams_preserve_messages_and_sender_credentials() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sock");
        let mut socket = Socket::bind(&path, 0o600)?;
        let sender = UnixDatagram::unbound()?;
        for description in ["one", "two"] {
            sender.send_to(
                &serde_json::to_vec(&serde_json::json!({
                    "description": description, "source": "test"
                }))?,
                &path,
            )?;
        }
        drop(sender); // Queued datagrams remain complete without a live client.
        for (index, description) in ["one", "two"].into_iter().enumerate() {
            let sequence = index as u64 + 1;
            let trigger = receive_trigger(&mut socket, sequence)?.unwrap();
            assert_eq!(trigger.description, description);
            assert_eq!(trigger.sequence, sequence);
            let peer = trigger.peer.unwrap();
            assert_eq!(peer.peer_pid, std::process::id() as i32);
            assert_eq!(peer.peer_uid, unsafe { libc::getuid() });
        }
        assert!(receive_trigger(&mut socket, 3)?.is_none());
        Ok(())
    }

    #[test]
    fn standalone_socket_reclaims_stale_path_but_not_live_or_regular_files() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sock");
        let live = UnixDatagram::bind(&path)?;
        assert!(Socket::bind(&path, 0o600).is_err());
        drop(live); // Simulate a crash: the socket inode remains.
        let socket = Socket::bind(&path, 0o600)?;
        assert!(Socket::bind(&path, 0o600).is_err());
        drop(socket);
        assert!(!path.exists());
        fs::write(&path, b"unrelated")?;
        assert!(Socket::bind(&path, 0o600).is_err());
        assert_eq!(fs::read(&path)?, b"unrelated");
        Ok(())
    }

    #[test]
    fn invalid_or_oversized_datagrams_do_not_consume_sequence_or_splice_requests() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sock");
        let mut socket = Socket::bind(&path, 0o600)?;
        let sender = UnixDatagram::unbound()?;
        let mut oversized = br#"{"source":"test"}"#.to_vec();
        oversized.resize(MAX_MESSAGE + 1, b' ');
        for invalid in [
            oversized.as_slice(),
            br#"{"source":"test","ack":true}"#,
            br#"{"description":"partial"#,
            br#""}"#,
            b"",
            br#"{} {}"#,
        ] {
            sender.send_to(invalid, &path)?;
            assert!(receive_trigger(&mut socket, 1)?.is_none());
        }
        let mut valid = br#"{"source":"test"}"#.to_vec();
        valid.resize(MAX_MESSAGE, b' ');
        sender.send_to(&valid, &path)?;
        assert_eq!(receive_trigger(&mut socket, 1)?.unwrap().sequence, 1);
        Ok(())
    }

    #[test]
    fn first_capture_metadata_stats_and_marker_share_one_staging_submission() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let socket_path = dir.path().join("sock");
        let capture_path = dir.path().join("capture.retroft");
        let directory = File::open(dir.path())?;
        let metadata = MetadataCache::new(&[
            (RecordKind::Metadata, br#"{"schema_version":1}"#.to_vec()),
            (
                RecordKind::MetadataFile,
                format::metadata_file_payload("events/header_page", b"kernel format")?,
            ),
        ])?;
        let mut storage = Storage::new(File::open("/dev/null")?, 16 * 1024, 2, 8 * 1024 * 1024)?;
        let mut socket = Socket::bind(&socket_path, 0o600)?;
        let client = UnixDatagram::unbound()?;
        client.send_to(
            br#"{"description":"persisted marker","source":"test-service"}"#,
            &socket_path,
        )?;
        let trigger = receive_trigger(&mut socket, 7)?.unwrap();
        let capture = start_capture(
            &mut storage,
            capture_path.clone(),
            &directory,
            &metadata,
            br#"{"schema_version":1,"phase":"start","cpus":[]}"#,
            CaptureStart {
                trigger: &trigger,
                clock: ClockSnapshot {
                    sample: crate::clock::sample()?,
                    reason: Reason::FileStart,
                },
                capture_index: 1,
                previous_capture_index: None,
            },
        )?;
        assert_eq!(storage.free_count(), 1);
        assert_eq!(storage.durable_prefix(), 0);
        assert_eq!(capture.record_sequence, 6);
        storage.finish()?;
        let bytes = fs::read(&capture_path)?;
        assert_eq!(&bytes[..format::FILE_HEADER_LEN], &format::FILE_HEADER);
        let mut offset = format::FILE_HEADER_LEN;
        for (index, kind) in [
            RecordKind::Metadata,
            RecordKind::MetadataFile,
            RecordKind::ClockSnapshot,
            RecordKind::Stats,
            RecordKind::Trigger,
        ]
        .iter()
        .enumerate()
        {
            let header = &bytes[offset..offset + HEADER_LEN];
            assert_eq!(&header[..4], b"FTRR");
            assert_eq!(u16::from_le_bytes(header[6..8].try_into()?), *kind as u16);
            assert_eq!(
                u64::from_le_bytes(header[16..24].try_into()?),
                index as u64 + 1
            );
            let length = u32::from_le_bytes(header[32..36].try_into()?) as usize;
            if *kind == RecordKind::Metadata {
                let manifest: serde_json::Value = serde_json::from_slice(
                    &bytes[offset + HEADER_LEN..offset + HEADER_LEN + length],
                )?;
                assert_eq!(manifest["capture_chain"]["schema_version"], 1);
                assert_eq!(manifest["capture_chain"]["capture_index"], "1");
                assert!(manifest["capture_chain"]["previous_capture_index"].is_null());
            }
            if *kind == RecordKind::Trigger {
                let marker: serde_json::Value = serde_json::from_slice(
                    &bytes[offset + HEADER_LEN..offset + HEADER_LEN + length],
                )?;
                assert_eq!(marker["description"], "persisted marker");
                assert_eq!(marker["source"], "test-service");
                assert_eq!(marker["peer_pid"], std::process::id());
                assert_eq!(marker["peer_uid"], unsafe { libc::getuid() });
                assert_eq!(marker["sequence"], "7");
            }
            offset += HEADER_LEN + length;
        }
        assert_eq!(offset, bytes.len());
        Ok(())
    }

    #[test]
    fn resume_history_waits_for_staging_and_is_never_appended_after_end() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("resume.retroft");
        let directory = File::open(dir.path())?;
        let metadata =
            MetadataCache::new(&[(RecordKind::Metadata, br#"{"schema_version":1}"#.to_vec())])?;
        let sample = crate::clock::ClockSample {
            monotonic_ns: 100,
            boottime_ns: 100,
            realtime_ns: 1000,
            uncertainty_ns: 10,
        };
        let history = [
            ClockSnapshot {
                sample,
                reason: Reason::BeforeResume,
            },
            ClockSnapshot {
                sample: crate::clock::ClockSample {
                    monotonic_ns: 200,
                    boottime_ns: 5_000_000_200,
                    realtime_ns: 5_000_001_100,
                    uncertainty_ns: 10,
                },
                reason: Reason::Resume,
            },
        ];
        let trigger = Trigger::parse(br#"{"source":"test","description":"after resume"}"#, 1, 300)?;
        let mut storage = Storage::new(File::open("/dev/null")?, 16 * 1024, 1, 8 * 1024 * 1024)?;
        let mut capture = start_capture(
            &mut storage,
            path.clone(),
            &directory,
            &metadata,
            br#"{"schema_version":1,"phase":"start","cpus":[]}"#,
            CaptureStart {
                trigger: &trigger,
                clock: ClockSnapshot {
                    sample,
                    reason: Reason::FileStart,
                },
                capture_index: 2,
                previous_capture_index: Some(1),
            },
        )?;
        assert!(clock_pending(&capture, &history));
        assert!(!stage_clock_history(&mut storage, &mut capture, &history)?);
        assert_eq!(capture.clock_history_written, 0);
        storage.finish()?;
        assert!(stage_clock_history(&mut storage, &mut capture, &history)?);
        assert_eq!(capture.clock_history_written, 1);
        assert!(!stage_clock_history(&mut storage, &mut capture, &history)?);
        storage.finish()?;
        assert!(stage_clock_history(&mut storage, &mut capture, &history)?);
        assert!(!clock_pending(&capture, &history));
        storage.finish()?;
        let bytes = fs::read(path)?;
        let first_payload = format::FILE_HEADER_LEN + HEADER_LEN;
        let first_length = u32::from_le_bytes(
            bytes[format::FILE_HEADER_LEN + 32..format::FILE_HEADER_LEN + 36].try_into()?,
        ) as usize;
        let manifest: serde_json::Value =
            serde_json::from_slice(&bytes[first_payload..first_payload + first_length])?;
        assert_eq!(manifest["capture_chain"]["capture_index"], "2");
        assert_eq!(manifest["capture_chain"]["previous_capture_index"], "1");
        let mut offset = format::FILE_HEADER_LEN;
        let mut reasons = Vec::new();
        let mut sequence = 1;
        while offset < bytes.len() {
            let header = &bytes[offset..offset + HEADER_LEN];
            assert_eq!(u64::from_le_bytes(header[16..24].try_into()?), sequence);
            sequence += 1;
            let length = u32::from_le_bytes(header[32..36].try_into()?) as usize;
            if u16::from_le_bytes(header[6..8].try_into()?) == RecordKind::ClockSnapshot as u16 {
                let snapshot: serde_json::Value = serde_json::from_slice(
                    &bytes[offset + HEADER_LEN..offset + HEADER_LEN + length],
                )?;
                reasons.push(snapshot["reason"].as_str().unwrap().to_owned());
                assert_eq!(
                    u64::from_le_bytes(header[24..32].try_into()?).to_string(),
                    snapshot["monotonic_ns"]
                );
            }
            offset += HEADER_LEN + length;
        }
        assert_eq!(reasons, ["file_start", "before_resume", "resume"]);
        capture.phase = CapturePhase::Closing;
        capture.clock_history_written = 0;
        let end = storage.logical_end();
        assert!(!clock_pending(&capture, &history));
        assert!(!stage_clock_history(&mut storage, &mut capture, &history)?);
        assert_eq!(storage.logical_end(), end);
        Ok(())
    }
}
