//! Offline-only raw ring-buffer decoder and native Perfetto protobuf exporter.
//!
//! Application binary interface (ABI) references: Linux include/linux/ring_buffer.h; libtraceevent's
//! src/kbuffer-parse.c; Perfetto protos/perfetto/trace/{trace_packet,ftrace/*}.proto.
//! No event interpretation is performed by the recorder.
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use crate::capture::CaptureReader;
use retroft_format::RecordKind;

const MAX_METADATA: usize = 64 * 1024 * 1024;
const MAX_SUBBUFFER: usize = 16 * 1024 * 1024;
const MAX_CLOCK_SNAPSHOTS: usize = 65536;
const TRIGGER_TRACK: u64 = 0x4654524143450001;
const FTRACE_MONOTONIC_CLOCK: u64 = 64;

#[derive(Debug, Deserialize)]
struct Manifest {
    schema_version: u32,
    #[serde(default)]
    kernel_release: Option<String>,
    #[serde(default)]
    architecture: Option<String>,
    endianness: String,
    word_size: usize,
    subbuffer_size: usize,
    trace_clock: String,
    cpus: Vec<u32>,
    events: Vec<EventMetadata>,
    header_page_ref: String,
    header_event_ref: String,
    #[serde(default)]
    initial_tasks: Vec<Task>,
    #[serde(default)]
    clock_monotonic_ns: Option<String>,
    #[serde(default)]
    clock_boottime_ns: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventMetadata {
    id: u32,
    system: String,
    name: String,
    format_ref: String,
}
#[derive(Debug, Deserialize)]
struct Task {
    tid: u32,
    tgid: u32,
    comm: String,
}

#[derive(Debug, Default, Serialize)]
pub struct CpuSummary {
    pub pages: u64,
    pub events: u64,
    #[serde(serialize_with = "decimal_option")]
    pub first_timestamp_ns: Option<u64>,
    #[serde(serialize_with = "decimal_option")]
    pub last_timestamp_ns: Option<u64>,
    #[serde(serialize_with = "decimal_option")]
    pub recovered_duration_ns: Option<u64>,
    pub lost_pages: u64,
    /// Raw input pages/events omitted by leading-overrun cleanup.
    pub omitted_pages: u64,
    pub omitted_events: u64,
    /// Kernel loss flags omitted with the prefix or suppressed at its boundary.
    pub leading_loss_pages: u64,
    /// None means no exact count was supplied by the kernel for one or more loss pages.
    #[serde(serialize_with = "decimal_option")]
    pub lost_events: Option<u64>,
}

fn decimal_option<S: serde::Serializer>(
    value: &Option<u64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ExportSummary {
    pub records: u64,
    pub events: u64,
    pub triggers: u64,
    pub clock_snapshots: u64,
    #[serde(serialize_with = "decimal_option")]
    pub maximum_clock_sample_uncertainty_ns: Option<u64>,
    pub generic_events: u64,
    pub unknown_event_ids: BTreeMap<u32, u64>,
    pub cpus: BTreeMap<u32, CpuSummary>,
    pub completed: bool,
    pub recovered_tail: Option<String>,
    pub last_accounting: Option<serde_json::Value>,
    pub capture_end: Option<serde_json::Value>,
    /// Issues or reduced guarantees identified for this export.
    pub warnings: Vec<String>,
    /// General interpretation caveats, not detected trace faults.
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClockSample {
    monotonic_ns: u64,
    boottime_ns: u64,
    realtime_ns: u64,
    uncertainty_ns: u64,
}

impl ClockSample {
    fn parse(payload: &[u8], timestamp_ns: u64) -> Result<Self> {
        ensure!(payload.len() <= 4096, "clock snapshot exceeds limit");
        let value: serde_json::Value = serde_json::from_slice(payload)?;
        ensure!(
            value["schema_version"].as_u64() == Some(1),
            "unsupported clock snapshot schema"
        );
        ensure!(
            matches!(
                value["reason"].as_str(),
                Some("file_start" | "before_resume" | "resume")
            ),
            "unsupported clock snapshot reason"
        );
        let number = |field: &str| -> Result<u64> {
            let text = value[field]
                .as_str()
                .with_context(|| format!("clock snapshot lacks decimal-string {field}"))?;
            let parsed: u64 = text
                .parse()
                .with_context(|| format!("invalid clock snapshot {field}"))?;
            ensure!(
                parsed.to_string() == text,
                "clock snapshot {field} is not a canonical decimal string"
            );
            Ok(parsed)
        };
        let sample = Self {
            monotonic_ns: number("monotonic_ns")?,
            boottime_ns: number("boottime_ns")?,
            realtime_ns: number("realtime_ns")?,
            uncertainty_ns: number("uncertainty_ns")?,
        };
        ensure!(
            sample.monotonic_ns == timestamp_ns,
            "clock snapshot/header timestamps disagree"
        );
        Ok(sample)
    }
}

pub(crate) fn validate_clock_snapshot(payload: &[u8], timestamp_ns: u64) -> Result<()> {
    ClockSample::parse(payload, timestamp_ns).map(|_| ())
}

/// Only clock samples are retained by the first pass. Native Perfetto clock
/// correlations extrapolate older history; resumed UTC correlations start at
/// their sampled times. Event intervals stay in the original monotonic clock.
fn clock_samples(input: &Path) -> Result<Vec<ClockSample>> {
    let mut reader = CaptureReader::open(input)?;
    let mut samples: Vec<ClockSample> = Vec::new();
    while let Some(record) = reader.next_record()? {
        if record.kind == RecordKind::End {
            break;
        }
        if record.kind == RecordKind::ClockSnapshot {
            ensure!(
                samples.len() < MAX_CLOCK_SNAPSHOTS,
                "too many clock snapshots"
            );
            let sample = ClockSample::parse(&record.payload, record.timestamp_ns)?;
            samples.push(sample);
        }
    }
    normalize_clock_samples(samples)
}

fn normalize_clock_samples(mut samples: Vec<ClockSample>) -> Result<Vec<ClockSample>> {
    ensure!(
        samples.len() <= MAX_CLOCK_SNAPSHOTS,
        "too many clock snapshots"
    );
    // A new file's start sample can precede retained resume history on disk.
    samples.sort_by_key(|sample| sample.monotonic_ns);
    samples.dedup();
    for pair in samples.windows(2) {
        ensure!(
            pair[1].monotonic_ns > pair[0].monotonic_ns
                && pair[1].boottime_ns > pair[0].boottime_ns,
            "conflicting/non-increasing clock snapshots"
        );
    }
    Ok(samples)
}

#[derive(Debug)]
struct ClockMapping {
    fallback_offset: Option<i128>,
}

impl ClockMapping {
    fn new(manifest: &Manifest, samples: &[ClockSample]) -> Result<Self> {
        let fallback_offset = match (&manifest.clock_monotonic_ns, &manifest.clock_boottime_ns) {
            (Some(mono), Some(boot)) => {
                Some(boot.parse::<u64>()? as i128 - mono.parse::<u64>()? as i128)
            }
            _ => None,
        };
        ensure!(
            manifest.trace_clock == "boot" || manifest.trace_clock == "mono",
            "unsupported trace clock {}: no trustworthy BOOTTIME mapping",
            manifest.trace_clock
        );
        ensure!(
            manifest.trace_clock != "mono" || !samples.is_empty() || fallback_offset.is_some(),
            "mono trace clock requires saved monotonic/boottime correlation"
        );
        Ok(Self { fallback_offset })
    }

    fn monotonic_to_boot(&self, timestamp: u64) -> Result<u64> {
        let offset = self
            .fallback_offset
            .context("recorder marker export requires monotonic/boottime clock samples")?;
        u64::try_from(timestamp as i128 + offset).context("clock conversion overflow")
    }
}

#[derive(Debug, Clone)]
struct Field {
    name: String,
    declaration: String,
    offset: usize,
    size: usize,
    signed: bool,
}

fn fields(text: &str) -> Result<BTreeMap<String, Field>> {
    let mut result = BTreeMap::new();
    for line in text.lines() {
        let Some((_, body)) = line.split_once("field:") else {
            continue;
        };
        let mut parts = body.split(';');
        let declaration = parts.next().context("missing field declaration")?.trim();
        let token = declaration
            .split_whitespace()
            .last()
            .context("empty field declaration")?;
        // Kernel emits both char comm[16] and __data_loc char[] name forms.
        let name = token
            .split('[')
            .next()
            .unwrap_or(token)
            .trim_start_matches('*');
        ensure!(!name.is_empty(), "empty field name");
        let mut values = BTreeMap::new();
        for part in parts {
            if let Some((key, value)) = part.split_once(':') {
                values.insert(key.trim(), value.trim());
            }
        }
        let number = |key: &str| -> Result<usize> {
            Ok(values
                .get(key)
                .with_context(|| format!("field {name}: missing {key}"))?
                .parse()?)
        };
        let signed = number("signed")?;
        ensure!(signed <= 1, "field {name}: invalid signed flag");
        let field = Field {
            name: name.into(),
            declaration: declaration.into(),
            offset: number("offset")?,
            size: number("size")?,
            signed: signed != 0,
        };
        ensure!(
            field.offset <= MAX_SUBBUFFER && field.size <= MAX_SUBBUFFER,
            "field {name} exceeds decoding limit"
        );
        ensure!(
            result.insert(name.into(), field).is_none(),
            "duplicate field {name}"
        );
    }
    ensure!(
        !result.is_empty() && result.len() <= 1024,
        "invalid field table"
    );
    Ok(result)
}

#[derive(Clone, Copy, Debug)]
enum Endian {
    Little,
    Big,
}
impl Endian {
    fn read(self, bytes: &[u8], offset: usize, size: usize) -> Result<u64> {
        ensure!(
            matches!(size, 1 | 2 | 4 | 8),
            "unsupported integer width {size}"
        );
        let data = bytes
            .get(offset..offset.checked_add(size).context("offset overflow")?)
            .context("integer exceeds record")?;
        let mut out = 0u64;
        match self {
            Self::Little => {
                for (i, byte) in data.iter().enumerate() {
                    out |= u64::from(*byte) << (i * 8);
                }
            }
            Self::Big => {
                for byte in data {
                    out = (out << 8) | u64::from(*byte);
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug)]
enum Value {
    String(String),
    Signed(i64),
    Unsigned(u64),
}
impl Field {
    fn value(&self, bytes: &[u8], endian: Endian) -> Result<Value> {
        let dynamic =
            self.declaration.contains("__data_loc") || self.declaration.contains("__rel_loc");
        let (start, len) = if dynamic {
            ensure!(self.size == 4, "invalid dynamic locator size");
            let loc = endian.read(bytes, self.offset, 4)? as usize;
            let base = if self.declaration.contains("__rel_loc") {
                self.offset + 4
            } else {
                0
            };
            (
                base.checked_add(loc & 0xffff).context("locator overflow")?,
                loc >> 16,
            )
        } else {
            (self.offset, self.size)
        };
        let data = bytes
            .get(start..start.checked_add(len).context("field overflow")?)
            .with_context(|| format!("field {} exceeds event", self.name))?;
        if self.declaration.contains("char") && (dynamic || self.declaration.contains('[')) {
            let end = data.iter().position(|b| *b == 0).unwrap_or(data.len());
            return Ok(Value::String(
                String::from_utf8_lossy(&data[..end]).into_owned(),
            ));
        }
        if dynamic || self.declaration.contains('[') || !matches!(len, 1 | 2 | 4 | 8) {
            // Preserve arrays as explicit hex bytes rather than pretending they are scalars.
            return Ok(Value::String(
                data.iter().map(|b| format!("{b:02x}")).collect(),
            ));
        }
        let number = endian.read(bytes, start, len)?;
        Ok(if self.signed {
            Value::Signed(((number << (64 - len * 8)) as i64) >> (64 - len * 8))
        } else {
            Value::Unsigned(number)
        })
    }
}

#[derive(Debug)]
struct EventFormat {
    system: String,
    name: String,
    fields: BTreeMap<String, Field>,
}
struct Decoder {
    endian: Endian,
    word_size: usize,
    subbuffer_size: usize,
    timestamp: Field,
    commit: Field,
    data: Field,
    formats: BTreeMap<u32, EventFormat>,
    clocks: ClockMapping,
    trace_is_monotonic: bool,
    preserve_monotonic: bool,
    native_syscalls: bool,
}

fn file_text<'a>(files: &'a BTreeMap<String, Vec<u8>>, path: &str) -> Result<&'a str> {
    std::str::from_utf8(
        files
            .get(path)
            .with_context(|| format!("missing preserved metadata {path}"))?,
    )
    .context("metadata is not UTF-8")
}

impl Decoder {
    #[cfg(test)]
    fn new(manifest: &Manifest, files: &BTreeMap<String, Vec<u8>>) -> Result<Self> {
        Self::with_clocks(manifest, files, &[])
    }

    fn with_clocks(
        manifest: &Manifest,
        files: &BTreeMap<String, Vec<u8>>,
        samples: &[ClockSample],
    ) -> Result<Self> {
        ensure!(
            manifest.schema_version == 1,
            "unsupported metadata schema {}",
            manifest.schema_version
        );
        ensure!(
            matches!(manifest.word_size, 4 | 8),
            "invalid target word size"
        );
        ensure!(
            (16..=MAX_SUBBUFFER).contains(&manifest.subbuffer_size),
            "invalid subbuffer size"
        );
        ensure!(
            manifest.events.len() <= 4096
                && manifest.cpus.len() <= 65536
                && manifest.initial_tasks.len() <= 65536,
            "manifest exceeds limits"
        );
        let endian = match manifest.endianness.as_str() {
            "little" => Endian::Little,
            "big" => Endian::Big,
            other => bail!("unsupported endianness {other}"),
        };
        let header = fields(file_text(files, &manifest.header_page_ref)?)?;
        let get = |name| {
            header
                .get(name)
                .cloned()
                .with_context(|| format!("header_page missing {name}"))
        };
        let timestamp = get("timestamp")?;
        let commit = get("commit")?;
        let data = get("data")?;
        ensure!(
            timestamp.size == 8 && commit.size == manifest.word_size,
            "header_page ABI contradicts manifest word size"
        );
        ensure!(
            data.offset <= manifest.subbuffer_size
                && data.size <= manifest.subbuffer_size - data.offset,
            "header_page data exceeds subbuffer"
        );
        // Accept the documented modern encoding only, fail on historical/new layouts.
        let event_header = file_text(files, &manifest.header_event_ref)?;
        let bit_width = |name: &str| -> Option<u32> {
            event_header.lines().find_map(|line| {
                let (field, body) = line.split_once(':')?;
                if field.trim() != name {
                    return None;
                }
                body.split_whitespace().next()?.parse().ok()
            })
        };
        ensure!(
            bit_width("type_len") == Some(5) && bit_width("time_delta") == Some(27),
            "unsupported ring-buffer event header"
        );
        let mut formats = BTreeMap::new();
        for event in &manifest.events {
            ensure!(
                event.id <= u16::MAX as u32,
                "event ID exceeds common_type width"
            );
            let text = file_text(files, &event.format_ref)?;
            let actual_id: u32 = text
                .lines()
                .find_map(|line| line.strip_prefix("ID:"))
                .context("format missing ID")?
                .trim()
                .parse()?;
            let actual_name = text
                .lines()
                .find_map(|line| line.strip_prefix("name:"))
                .context("format missing name")?
                .trim();
            ensure!(
                actual_id == event.id && actual_name == event.name,
                "event metadata and preserved format disagree"
            );
            let parsed = fields(text)?;
            let common_type = parsed
                .get("common_type")
                .context("event missing common_type")?;
            // ftrace event IDs are the first native u16; enforce this common ABI.
            ensure!(
                common_type.offset == 0 && common_type.size == 2,
                "unsupported common_type ABI"
            );
            ensure!(
                parsed
                    .get("common_pid")
                    .is_some_and(|field| field.size == 4),
                "event common_pid must be 32-bit"
            );
            ensure!(
                formats
                    .insert(
                        event.id,
                        EventFormat {
                            system: event.system.clone(),
                            name: event.name.clone(),
                            fields: parsed
                        }
                    )
                    .is_none(),
                "duplicate event ID"
            );
        }
        let clocks = ClockMapping::new(manifest, samples)?;
        Ok(Self {
            endian,
            native_syscalls: syscall_machine(manifest.architecture.as_deref()).is_some(),
            word_size: manifest.word_size,
            subbuffer_size: manifest.subbuffer_size,
            timestamp,
            commit,
            data,
            formats,
            clocks,
            trace_is_monotonic: manifest.trace_clock == "mono",
            preserve_monotonic: manifest.trace_clock == "mono" && !samples.is_empty(),
        })
    }

    fn boot_time(&self, timestamp: u64) -> Result<u64> {
        if self.trace_is_monotonic && !self.preserve_monotonic {
            self.clocks.monotonic_to_boot(timestamp)
        } else {
            Ok(timestamp)
        }
    }

    fn page(&self, bytes: &[u8], skip_bytes: usize, committed: usize) -> Result<DecodedPage> {
        ensure!(
            bytes.len() == self.subbuffer_size,
            "raw record size {} differs from subbuffer size {}",
            bytes.len(),
            self.subbuffer_size
        );
        let mut timestamp = self
            .endian
            .read(bytes, self.timestamp.offset, self.timestamp.size)?;
        let commit = self
            .endian
            .read(bytes, self.commit.offset, self.commit.size)?;
        let header_committed = (commit & ((1 << 27) - 1)) as usize;
        ensure!(
            committed <= header_committed,
            "envelope committed bytes exceed raw header"
        );
        ensure!(
            committed <= self.data.size && committed <= bytes.len() - self.data.offset,
            "committed bytes exceed page"
        );
        // A continuation of an already-exported page must not count its
        // original loss marker a second time.
        let lost = skip_bytes == 0 && commit & (1 << 31) != 0;
        let lost_events = if lost && commit & (1 << 30) != 0 && committed == header_committed {
            Some(
                self.endian
                    .read(bytes, self.data.offset + committed, self.word_size)?,
            )
        } else if lost {
            None
        } else {
            Some(0)
        };
        ensure!(
            skip_bytes <= committed && skip_bytes.is_multiple_of(4),
            "invalid duplicate prefix"
        );
        let data = &bytes[self.data.offset..self.data.offset + committed];
        let mut cursor = 0;
        let mut events = Vec::new();
        let mut skip_is_boundary = skip_bytes == 0;
        while cursor < data.len() {
            if cursor == skip_bytes {
                skip_is_boundary = true;
            }
            let header = self.endian.read(data, cursor, 4)? as u32;
            let (kind, delta) = match self.endian {
                Endian::Little => (header & 31, header >> 5),
                Endian::Big => (header >> 27, header & ((1 << 27) - 1)),
            };
            let start = cursor;
            cursor += 4;
            match kind {
                30 | 31 => {
                    let extended = (self.endian.read(data, cursor, 4)? << 27) | u64::from(delta);
                    cursor += 4;
                    timestamp = if kind == 31 {
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
                        let length = self.endian.read(data, cursor, 4)? as usize;
                        ensure!(
                            length >= 4 && length.is_multiple_of(4),
                            "invalid padding length"
                        );
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
                        let length = self.endian.read(data, cursor, 4)? as usize;
                        ensure!(length >= 4, "invalid extended data length");
                        cursor += 4;
                        (length - 4).checked_add(3).context("length overflow")? & !3
                    } else {
                        kind as usize * 4
                    };
                    ensure!(length >= 2, "empty data event");
                    let end = cursor
                        .checked_add(length)
                        .context("event length overflow")?;
                    let payload = data
                        .get(cursor..end)
                        .context("event exceeds committed region")?;
                    if start >= skip_bytes {
                        let id = self.endian.read(payload, 0, 2)? as u32;
                        events.push(DecodedEvent {
                            timestamp: self.boot_time(timestamp)?,
                            id,
                            payload: payload.to_vec(),
                        });
                    }
                    cursor = end;
                }
                _ => unreachable!(),
            }
            ensure!(cursor <= data.len(), "ring record exceeds committed region");
        }
        skip_is_boundary |= skip_bytes == committed;
        ensure!(
            skip_is_boundary,
            "duplicate prefix is not a ring-record boundary"
        );
        Ok(DecodedPage {
            events,
            lost,
            lost_events,
        })
    }
}

struct DecodedEvent {
    timestamp: u64,
    id: u32,
    payload: Vec<u8>,
}
struct DecodedPage {
    events: Vec<DecodedEvent>,
    lost: bool,
    lost_events: Option<u64>,
}

// Small protobuf wire writer. Message/bytes use the same length-delimited wire
// type; all field IDs below are taken from the official Perfetto .proto files.
// It avoids protoc and a target-side protobuf dependency.
#[derive(Default)]
struct Proto(Vec<u8>);
impl Proto {
    fn varint(&mut self, mut value: u64) {
        while value >= 128 {
            self.0.push((value as u8 & 127) | 128);
            value >>= 7;
        }
        self.0.push(value as u8);
    }
    fn uint(&mut self, tag: u32, value: u64) {
        self.varint(u64::from(tag) << 3);
        self.varint(value);
    }
    fn bytes(&mut self, tag: u32, value: &[u8]) {
        self.varint((u64::from(tag) << 3) | 2);
        self.varint(value.len() as u64);
        self.0.extend_from_slice(value);
    }
    fn string(&mut self, tag: u32, value: &str) {
        self.bytes(tag, value.as_bytes());
    }
    fn message(&mut self, tag: u32, value: Proto) {
        self.bytes(tag, &value.0);
    }
}

fn write_packet(writer: &mut impl Write, packet: Proto) -> Result<()> {
    let mut trace = Proto::default();
    trace.message(1, packet);
    writer.write_all(&trace.0)?;
    Ok(())
}
fn packet(tag: u32, message: Proto, timestamp: Option<u64>) -> Proto {
    packet_with_clock(tag, message, timestamp, 6)
}
fn packet_with_clock(tag: u32, message: Proto, timestamp: Option<u64>, clock_id: u64) -> Proto {
    let mut out = Proto::default();
    out.message(tag, message);
    out.uint(10, 1);
    if let Some(timestamp) = timestamp {
        out.uint(8, timestamp);
        out.uint(58, clock_id);
    }
    out
}

fn number(value: Value) -> Result<u64> {
    match value {
        Value::Signed(v) => Ok(v as u64),
        Value::Unsigned(v) => Ok(v),
        _ => bail!("expected numeric event field"),
    }
}

// (FtraceEvent field, event field names in protobuf field order).
fn typed_event(system: &str, name: &str) -> Option<(u32, &'static [&'static str])> {
    Some(match (system, name) {
        ("sched", "sched_switch") => (
            4,
            &[
                "prev_comm",
                "prev_pid",
                "prev_prio",
                "prev_state",
                "next_comm",
                "next_pid",
                "next_prio",
            ],
        ),
        ("sched", "sched_wakeup") => (17, &["comm", "pid", "prio", "success", "target_cpu"]),
        ("sched", "sched_waking") => (20, &["comm", "pid", "prio", "success", "target_cpu"]),
        ("sched", "sched_wakeup_new") => (114, &["comm", "pid", "prio", "success", "target_cpu"]),
        ("irq", "softirq_entry") => (24, &["vec"]),
        ("irq", "softirq_exit") => (25, &["vec"]),
        ("irq", "softirq_raise") => (26, &["vec"]),
        ("irq", "irq_handler_entry") => (36, &["irq", "name", "handler"]),
        ("irq", "irq_handler_exit") => (37, &["irq", "ret"]),
        ("sched", "sched_process_exec") => (237, &["filename", "pid", "old_pid"]),
        ("sched", "sched_process_exit") => (238, &["comm", "pid", "tgid", "prio"]),
        ("sched", "sched_process_fork") => (
            239,
            &["parent_comm", "parent_pid", "child_comm", "child_pid"],
        ),
        ("sched", "sched_process_free") => (240, &["comm", "pid", "prio"]),
        _ => return None,
    })
}

fn encode_event(
    decoder: &Decoder,
    event: &DecodedEvent,
    summary: &mut ExportSummary,
) -> Result<Option<Proto>> {
    let Some(format) = decoder.formats.get(&event.id) else {
        *summary.unknown_event_ids.entry(event.id).or_default() += 1;
        return Ok(None);
    };
    let mut out = Proto::default();
    out.uint(1, event.timestamp);
    let pid = number(format.fields["common_pid"].value(&event.payload, decoder.endian)?)?;
    ensure!(pid <= u32::MAX as u64, "invalid common_pid");
    out.uint(2, pid);
    // FtraceEvent.common_flags (tag 5) is a schema placeholder, not a
    // supported trace field. Emitting it produces unnamed ftrace rows in
    // Trace Processor and breaks the UI's non-null event-name assumption.
    // The original flags remain available in the raw Retroft capture.
    if let Some((tag, names)) = typed_event(&format.system, &format.name) {
        let mut typed = Proto::default();
        for (index, name) in names.iter().enumerate() {
            if let Some(field) = format.fields.get(*name) {
                let tag = index as u32 + 1;
                match field.value(&event.payload, decoder.endian)? {
                    Value::String(v) => typed.string(tag, &v),
                    value => typed.uint(tag, number(value)?),
                }
            } else if *name != "success" && *name != "tgid" && *name != "handler" {
                bail!(
                    "{}:{} is missing required field {name}",
                    format.system,
                    format.name
                );
            }
        }
        out.message(tag, typed);
    } else {
        summary.generic_events += 1;
        let mut generic = Proto::default();
        generic.string(1, &format!("{}:{}", format.system, format.name));
        for field in format
            .fields
            .values()
            .filter(|field| !field.name.starts_with("common_"))
        {
            let mut encoded = Proto::default();
            encoded.string(1, &field.name);
            match field.value(&event.payload, decoder.endian)? {
                Value::String(v) => encoded.string(3, &v),
                Value::Signed(v) => encoded.uint(4, v as u64),
                Value::Unsigned(v) => encoded.uint(5, v),
            }
            generic.message(2, encoded);
        }
        out.message(327, generic);
    }
    Ok(Some(out))
}

// Rust's recorded architecture names differ from uname on ARM. Perfetto uses
// utsname.machine to select its native-ABI syscall-number table.
fn syscall_machine(architecture: Option<&str>) -> Option<&'static str> {
    match architecture? {
        "x86_64" => Some("x86_64"),
        "aarch64" => Some("aarch64"),
        "arm" => Some("armv7l"),
        _ => None,
    }
}

fn encode_syscall(decoder: &Decoder, event: &DecodedEvent) -> Result<Option<Proto>> {
    if !decoder.native_syscalls {
        return Ok(None);
    }
    let Some(format) = decoder.formats.get(&event.id) else {
        return Ok(None);
    };
    if format.system != "syscalls" {
        return Ok(None);
    }
    let enter = format.name.starts_with("sys_enter_");
    if !enter && !format.name.starts_with("sys_exit_") {
        return Ok(None);
    }
    let nr = format
        .fields
        .get("__syscall_nr")
        .context("named syscall lacks __syscall_nr")?;
    let mut syscall = Proto::default();
    syscall.uint(1, number(nr.value(&event.payload, decoder.endian)?)?);
    if enter {
        // Kernel argument slots are in ABI order, not alphabetical field order.
        // Additional dynamic string fields remain in the original generic event.
        let mut arguments: Vec<_> = format
            .fields
            .values()
            .filter(|field| {
                !field.name.starts_with("common_")
                    && field.name != "__syscall_nr"
                    && !field.declaration.contains("__data_loc")
                    && !field.declaration.contains("__rel_loc")
            })
            .collect();
        arguments.sort_by_key(|field| field.offset);
        ensure!(arguments.len() <= 6, "too many named syscall arguments");
        for field in arguments {
            syscall.uint(2, number(field.value(&event.payload, decoder.endian)?)?);
        }
    } else {
        let ret = format
            .fields
            .get("ret")
            .context("named syscall lacks ret")?;
        syscall.uint(2, number(ret.value(&event.payload, decoder.endian)?)?);
    }
    let mut out = Proto::default();
    out.uint(1, event.timestamp);
    let pid = number(format.fields["common_pid"].value(&event.payload, decoder.endian)?)?;
    ensure!(pid <= u32::MAX as u64, "invalid common_pid");
    out.uint(2, pid);
    // FtraceEvent's payload is a oneof: native and generic representations
    // must be separate events, even though they describe the same source record.
    out.message(if enter { 329 } else { 330 }, syscall);
    Ok(Some(out))
}

fn track_descriptor() -> Proto {
    let mut track = Proto::default();
    track.uint(1, TRIGGER_TRACK);
    track.string(2, "Recorder triggers and accounting");
    packet(60, track, None)
}
fn accounting_marker(timestamp: u64, value: &serde_json::Value, clock_id: u64) -> Result<Proto> {
    let name = match value["phase"].as_str() {
        Some("start") => "Capture start",
        Some("end") => "Capture end",
        _ => "Capture accounting",
    };
    let mut event = marker_event(name);
    for (key, value) in value.as_object().context("accounting must be an object")? {
        event.message(4, accounting_argument(Some(key), value));
    }
    Ok(packet_with_clock(11, event, Some(timestamp), clock_id))
}

// Native nested DebugAnnotations give the details panel expandable CPU/field
// groups. Keep decimal strings exact and omit redundant original stats text.
fn accounting_argument(name: Option<&str>, value: &serde_json::Value) -> Proto {
    use serde_json::Value;
    let mut annotation = Proto::default();
    if let Some(name) = name {
        annotation.string(10, name);
    }
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if key != "raw" {
                    annotation.message(11, accounting_argument(Some(key), value));
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                annotation.message(12, accounting_argument(None, value));
            }
        }
        Value::String(text) => annotation.string(6, text),
        Value::Bool(value) => annotation.uint(2, u64::from(*value)),
        Value::Number(number) => {
            if let Some(value) = number.as_u64() {
                annotation.uint(3, value);
            } else if let Some(value) = number.as_i64() {
                annotation.uint(4, value as u64);
            } else if let Some(value) = number.as_f64() {
                annotation.varint((5 << 3) | 1); // double_value: fixed64 wire type.
                annotation.0.extend_from_slice(&value.to_le_bytes());
            }
        }
        Value::Null => annotation.string(6, "unknown"),
    }
    annotation
}

fn marker_event(name: &str) -> Proto {
    let mut event = Proto::default();
    event.uint(9, 3);
    event.uint(11, TRIGGER_TRACK);
    event.string(23, name);
    event.string(22, "Retroft");
    event
}

fn trigger_marker(timestamp: u64, value: &serde_json::Value, clock_id: u64) -> Result<Proto> {
    let sequence = value["sequence"]
        .as_str()
        .context("trigger lacks decimal-string sequence")?;
    let source = value["source"].as_str().filter(|s| !s.is_empty());
    let description = value["description"].as_str().unwrap_or("");
    let name = match source {
        Some(source) => format!("Trigger {sequence} [{source}]: {description}"),
        None => format!("Trigger {sequence}: {description}"),
    };
    let mut event = marker_event(&name);
    // TrackEvent.debug_annotations=4; DebugAnnotation name=10, string=6, uint=3.
    // See Perfetto protos/perfetto/trace/track_event/debug_annotation.proto.
    // Connector credentials annotate this recorder track, never a process track.
    for key in [
        "source",
        "description",
        "peer_process_name",
        "peer_pid",
        "peer_uid",
        "peer_gid",
    ] {
        let mut annotation = Proto::default();
        annotation.string(10, key);
        if let Some(text) = value[key].as_str() {
            annotation.string(6, text);
        } else if let Some(number) = value[key].as_u64() {
            annotation.uint(3, number);
        } else {
            continue;
        }
        event.message(4, annotation);
    }
    Ok(packet_with_clock(11, event, Some(timestamp), clock_id))
}

fn kernel_info(release: &str, machine: Option<&str>) -> Proto {
    // TracePacket.system_info=45, SystemInfo.utsname=1, Utsname.release=3.
    // Perfetto needs the recorded kernel version to decode sched_switch states.
    let mut utsname = Proto::default();
    utsname.string(1, "Linux");
    utsname.string(3, release);
    if let Some(machine) = machine {
        utsname.string(4, machine);
    }
    let mut info = Proto::default();
    info.message(1, utsname);
    packet(45, info, None)
}

fn source_capture_metadata(inputs: &[&Path]) -> Result<Proto> {
    let paths = inputs
        .iter()
        .map(|path| std::path::absolute(path).map(|path| path.to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    // TracePacket.chrome_events=5, ChromeEventBundle.metadata=2.
    // This arbitrary string metadata appears in Perfetto's metadata table;
    // the importer prefixes its name with "cr-". JSON preserves input order.
    let mut entry = Proto::default();
    entry.string(1, "retroft.source_capture_paths");
    entry.string(2, &serde_json::to_string(&paths)?);
    let mut bundle = Proto::default();
    bundle.message(2, entry);
    Ok(packet(5, bundle, None))
}

fn initial_tasks(manifest: &Manifest) -> Proto {
    let mut tree = Proto::default();
    for task in &manifest.initial_tasks {
        let mut thread = Proto::default();
        thread.uint(1, task.tid as u64);
        thread.string(2, &task.comm);
        thread.uint(3, task.tgid as u64);
        tree.message(2, thread);
        if task.tid == task.tgid {
            let mut process = Proto::default();
            process.uint(1, task.tgid as u64);
            process.string(3, &task.comm);
            process.uint(9, 1);
            tree.message(1, process);
        }
    }
    packet(2, tree, None)
}

fn clock_snapshot(sample: &ClockSample, preserve_monotonic: bool) -> Proto {
    let mut snapshot = Proto::default();
    for (id, timestamp) in [
        (1, sample.realtime_ns),
        (3, sample.monotonic_ns),
        (6, sample.boottime_ns),
    ] {
        let mut clock = Proto::default();
        clock.uint(1, id);
        clock.uint(2, timestamp);
        snapshot.message(1, clock);
    }
    if preserve_monotonic {
        let mut ftrace_clock = Proto::default();
        ftrace_clock.uint(1, FTRACE_MONOTONIC_CLOCK);
        ftrace_clock.uint(2, sample.monotonic_ns);
        snapshot.message(1, ftrace_clock);
    }
    snapshot.uint(2, if preserve_monotonic { 3 } else { 6 });
    packet(6, snapshot, None)
}

/// Convert a valid prefix of a capture. Memory is bounded by preserved metadata
/// and one raw sub-buffer, plus bounded clock samples collected in a first pass.
/// Event packets are streamed, not accumulated/sorted in RAM.
/// Existing output files are refused to prevent destroying an input capture.
/// Check the chain before creating output. Missing or damaged files must not
/// silently become scheduling intervals spanning unknown amounts of history.
fn verified_chain(inputs: &[&Path]) -> Result<Vec<ClockSample>> {
    let mut previous_index: Option<u64> = None;
    let mut common_manifest = None;
    let mut common_files = None;
    let mut samples = Vec::new();
    for input in inputs {
        let mut reader = CaptureReader::open(input)?;
        let mut manifest = None;
        let mut files = Vec::new();
        let mut metadata_bytes = 0usize;
        let mut started = false;
        let mut end = None;
        while let Some(record) = reader.next_record()? {
            ensure!(end.is_none(), "record after capture end");
            ensure!(!record.recovered_after_gap, "cannot join a corrupt capture");
            match record.kind {
                RecordKind::Metadata | RecordKind::MetadataFile => {
                    ensure!(!started, "late capture metadata");
                    metadata_bytes = metadata_bytes
                        .checked_add(record.payload.len())
                        .context("metadata length overflow")?;
                    ensure!(
                        metadata_bytes <= MAX_METADATA,
                        "preserved metadata exceeds limit"
                    );
                    if record.kind == RecordKind::Metadata {
                        ensure!(manifest.is_none(), "duplicate manifest");
                        manifest = Some(serde_json::from_slice::<serde_json::Value>(
                            &record.payload,
                        )?);
                    } else {
                        files.push(record.payload);
                    }
                }
                RecordKind::End => {
                    started = true;
                    end = Some(serde_json::from_slice::<serde_json::Value>(
                        &record.payload,
                    )?);
                }
                RecordKind::ClockSnapshot => {
                    started = true;
                    ensure!(
                        samples.len() < MAX_CLOCK_SNAPSHOTS,
                        "too many clock snapshots"
                    );
                    samples.push(ClockSample::parse(&record.payload, record.timestamp_ns)?);
                }
                _ => started = true,
            }
        }
        ensure!(
            reader.recovered_tail.is_none(),
            "cannot join a truncated or corrupt capture: {}",
            input.display()
        );
        let end = end.context("cannot join capture without a completion record")?;
        ensure!(
            end["schema_version"] == 1 && end["completed"] == true,
            "cannot join incomplete capture"
        );
        let mut manifest = manifest.context("capture lacks metadata manifest")?;
        ensure!(
            manifest["session_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty()),
            "capture lacks recorder session identity"
        );
        let chain = &manifest["capture_chain"];
        ensure!(
            chain["schema_version"] == 1,
            "capture lacks supported capture-chain metadata"
        );
        let index_text = chain["capture_index"]
            .as_str()
            .context("capture lacks capture index")?;
        let index: u64 = index_text.parse()?;
        ensure!(
            index > 0 && index.to_string() == index_text,
            "invalid capture index"
        );
        ensure!(
            end["capture_index"].as_str() == Some(index_text),
            "completion capture index mismatch"
        );
        let expected_previous = index
            .checked_sub(1)
            .filter(|value| *value > 0)
            .map(|value| value.to_string());
        ensure!(
            chain.get("previous_capture_index").is_some()
                && chain["previous_capture_index"] == serde_json::to_value(&expected_previous)?,
            "invalid predecessor capture index"
        );
        if let Some(previous) = previous_index {
            ensure!(previous.checked_add(1) == Some(index),
                "captures must be supplied in consecutive order: expected index after {previous}, got {index}");
        }
        previous_index = Some(index);
        manifest
            .as_object_mut()
            .context("invalid manifest")?
            .remove("capture_chain");
        if let Some(common) = &common_manifest {
            ensure!(
                *common == manifest,
                "captures belong to different recorder sessions or have different metadata"
            );
            ensure!(
                common_files.as_ref() == Some(&files),
                "capture event formats differ"
            );
        } else {
            common_manifest = Some(manifest);
            common_files = Some(files);
        }
    }
    normalize_clock_samples(samples)
}

// Infer conservative per-CPU valid-from boundaries without retaining history.
// Unlike Perfetto's recorder, we do not know the last event in an overwritten
// bundle. The first retained decoded event is a safe, slightly later boundary.
fn leading_history_boundaries(
    inputs: &[&Path],
    decoder: &Decoder,
    cpus: &[u32],
) -> Result<BTreeMap<u32, u64>> {
    let mut boundaries = BTreeMap::new();
    for input in inputs {
        let mut reader = CaptureReader::open(input)?;
        while let Some(record) = reader.next_record()? {
            if record.kind == RecordKind::End {
                break;
            }
            if record.kind != RecordKind::Trace || boundaries.contains_key(&record.cpu) {
                continue;
            }
            ensure!(
                cpus.contains(&record.cpu),
                "trace CPU missing from manifest"
            );
            let page = decoder
                .page(
                    &record.payload,
                    record.skip_bytes as usize,
                    record.committed_bytes as usize,
                )
                .with_context(|| {
                    format!("CPU {} record {} raw page", record.cpu, record.sequence)
                })?;
            if let Some(event) = page.events.first() {
                boundaries.insert(record.cpu, event.timestamp);
            }
            if cpus.iter().all(|cpu| boundaries.contains_key(cpu)) {
                return Ok(boundaries);
            }
        }
    }
    Ok(boundaries)
}

/// Controls optional cleanup of overwritten leading history.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExportOptions {
    pub raw: bool,
}

// A loss on an empty page belongs to its next actual surviving event, including
// an mmap continuation. Record ordinals remain stable across joined files.
fn leading_overruns(
    inputs: &[&Path],
    decoder: &Decoder,
    boundaries: &BTreeMap<u32, u64>,
    cpus: &[u32],
) -> Result<BTreeMap<u32, u64>> {
    if cpus.iter().any(|cpu| !boundaries.contains_key(cpu)) {
        return Ok(BTreeMap::new());
    }
    let Some(boundary) = boundaries.values().max().copied() else {
        return Ok(BTreeMap::new());
    };
    let mut pending = BTreeMap::new();
    let mut selected = BTreeMap::new();
    let mut ordinal = 0u64;
    for input in inputs {
        let mut reader = CaptureReader::open(input)?;
        while let Some(record) = reader.next_record()? {
            if record.kind == RecordKind::End {
                break;
            }
            if record.kind != RecordKind::Trace {
                continue;
            }
            ordinal += 1;
            let page = decoder.page(
                &record.payload,
                record.skip_bytes as usize,
                record.committed_bytes as usize,
            )?;
            if page.lost {
                pending.insert(record.cpu, ordinal);
            }
            if let Some(event) = page.events.first() {
                if let Some(loss) = pending.remove(&record.cpu) {
                    if event.timestamp <= boundary {
                        selected.insert(record.cpu, loss);
                    }
                }
            }
        }
    }
    Ok(selected)
}

/// Export one capture, including a recoverable prefix of an unfinished file.
pub fn export(input: &Path, output: &Path) -> Result<ExportSummary> {
    export_captures(&[input], output)
}

/// Join consecutive, complete captures from one recorder session into one
/// Perfetto stream. Decoder frontiers cross file boundaries; compression
/// dictionaries remain private to each file. One input retains legacy recovery.
pub fn export_captures(inputs: &[&Path], output: &Path) -> Result<ExportSummary> {
    export_captures_with_options(inputs, output, ExportOptions::default())
}

pub fn export_captures_with_options(
    inputs: &[&Path],
    output: &Path,
    options: ExportOptions,
) -> Result<ExportSummary> {
    ensure!(!inputs.is_empty(), "at least one capture is required");
    let samples = if inputs.len() == 1 {
        clock_samples(inputs[0])?
    } else {
        verified_chain(inputs)?
    };
    let mut manifest: Option<Manifest> = None;
    let mut files = BTreeMap::new();
    let mut metadata_bytes = 0usize;
    let mut decoder = None;
    let mut writer: Option<BufWriter<File>> = None;
    let mut summary = ExportSummary {
        clock_snapshots: samples.len() as u64,
        maximum_clock_sample_uncertainty_ns: samples
            .iter()
            .map(|sample| sample.uncertainty_ns)
            .max(),
        ..ExportSummary::default()
    };
    if options.raw {
        summary.warnings.push("Raw export preserves overwritten leading history and loss flags and omits leading-history hints; Perfetto may infer misleading scheduling gaps and runnable states.".into());
    }
    let mut last_pages: BTreeMap<u32, (u64, u32)> = BTreeMap::new();
    let mut pending_recovery_loss = BTreeSet::new();
    let mut pending_kernel_loss = BTreeSet::new();
    let mut pending_export_recovery_loss = BTreeSet::new();
    let mut leading_overrun_records = BTreeMap::new();
    let mut trace_ordinal = 0u64;
    for (file_index, input) in inputs.iter().enumerate() {
        let mut reader = CaptureReader::open(input)?;
        let mut saw_end = false;
        while let Some(record) = reader.next_record()? {
            ensure!(!saw_end, "record after capture end");
            summary.records += 1;
            // The preflight checked these against the first file. Replaying its
            // task snapshot or resetting the decoder would break cross-file state.
            if file_index > 0
                && matches!(record.kind, RecordKind::Metadata | RecordKind::MetadataFile)
            {
                continue;
            }
            if record.recovered_after_gap {
                last_pages.clear();
                pending_recovery_loss.extend(summary.cpus.keys().copied());
                summary.warnings.push(format!(
                "Corrupt records skipped before sequence {}; per-CPU continuity and loss counts are unknown.",
                record.sequence
            ));
                for cpu in summary.cpus.values_mut() {
                    cpu.lost_events = None;
                }
            }
            match record.kind {
                RecordKind::Metadata => {
                    ensure!(
                        manifest.is_none() && decoder.is_none(),
                        "duplicate/late manifest"
                    );
                    ensure!(
                        record.payload.len() <= MAX_METADATA,
                        "manifest exceeds limit"
                    );
                    metadata_bytes = record.payload.len();
                    manifest = Some(
                        serde_json::from_slice(&record.payload)
                            .context("invalid metadata manifest")?,
                    );
                }
                RecordKind::MetadataFile => {
                    ensure!(decoder.is_none(), "metadata file after trace stream began");
                    ensure!(record.payload.len() >= 4, "short metadata file record");
                    let length =
                        u32::from_le_bytes(record.payload[..4].try_into().unwrap()) as usize;
                    ensure!(
                        length <= 4096 && length <= record.payload.len() - 4,
                        "invalid metadata path length"
                    );
                    let path = std::str::from_utf8(&record.payload[4..4 + length])?.to_owned();
                    metadata_bytes = metadata_bytes
                        .checked_add(record.payload.len())
                        .context("metadata length overflow")?;
                    ensure!(
                        metadata_bytes <= MAX_METADATA,
                        "preserved metadata exceeds limit"
                    );
                    ensure!(
                        files
                            .insert(path, record.payload[4 + length..].to_vec())
                            .is_none(),
                        "duplicate metadata file"
                    );
                }
                _ => {
                    if decoder.is_none() {
                        let manifest = manifest
                            .as_ref()
                            .context("capture lacks metadata manifest")?;
                        decoder = Some(Decoder::with_clocks(manifest, &files, &samples)?);
                        let boundaries = leading_history_boundaries(
                            inputs,
                            decoder.as_ref().unwrap(),
                            &manifest.cpus,
                        )?;
                        if !options.raw {
                            leading_overrun_records = leading_overruns(
                                inputs,
                                decoder.as_ref().unwrap(),
                                &boundaries,
                                &manifest.cpus,
                            )?;
                        }
                        let file = OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(output)
                            .with_context(|| {
                                format!("create {} (existing files are refused)", output.display())
                            })?;
                        let mut output = BufWriter::new(file);
                        write_packet(&mut output, source_capture_metadata(inputs)?)?;
                        if let Some(release) = &manifest.kernel_release {
                            write_packet(
                                &mut output,
                                kernel_info(
                                    release,
                                    syscall_machine(manifest.architecture.as_deref()),
                                ),
                            )?;
                        } else {
                            summary.warnings.push("Capture has no recorded kernel release; Perfetto's legacy scheduler-state mapping may mislabel preemption and idle states.".into());
                        }
                        if manifest.kernel_release.is_none() {
                            write_packet(
                                &mut output,
                                kernel_info("", syscall_machine(manifest.architecture.as_deref())),
                            )?;
                        }
                        if syscall_machine(manifest.architecture.as_deref()).is_none()
                            && manifest
                                .events
                                .iter()
                                .any(|event| event.system == "syscalls")
                        {
                            summary.warnings.push("Unknown or missing recording architecture; named syscalls are exported as generic events only.".into());
                        }
                        // All correlations precede events, including correlations
                        // captured later than the ring history they describe.
                        for sample in &samples {
                            write_packet(
                                &mut output,
                                clock_snapshot(
                                    sample,
                                    decoder.as_ref().unwrap().preserve_monotonic,
                                ),
                            )?;
                        }
                        write_packet(&mut output, track_descriptor())?;
                        write_packet(&mut output, initial_tasks(manifest))?;
                        for cpu in &manifest.cpus {
                            if let Some(timestamp) = boundaries.get(cpu).filter(|_| !options.raw) {
                                // Publish all CPU hints before any events: Perfetto
                                // caches their maximum when parsing its first event.
                                let mut bundle = Proto::default();
                                bundle.uint(1, *cpu as u64);
                                // FtraceEventBundle.previous_bundle_end_timestamp (v47+).
                                bundle.uint(10, *timestamp);
                                if decoder.as_ref().unwrap().preserve_monotonic {
                                    bundle.uint(5, 2);
                                }
                                write_packet(&mut output, packet(1, bundle, None))?;
                            } else if !boundaries.contains_key(cpu) {
                                summary.warnings.push(format!(
                                    "CPU {cpu} has no retained events; its leading-history coverage cannot be established."
                                ));
                            }
                            summary.cpus.insert(
                                *cpu,
                                CpuSummary {
                                    lost_events: None,
                                    ..CpuSummary::default()
                                },
                            );
                        }
                        writer = Some(output);
                        files.clear();
                        summary
                            .limitations
                            .push("Initial /proc identity snapshot races process changes.".into());
                        if samples.is_empty() {
                            summary.warnings.push("Legacy BOOTTIME mapping uses startup samples and assumes no suspend during the capture; no UTC correlation is available.".into());
                        } else {
                            summary.limitations.push("UTC mapping is approximate: samples extrapolate older history and resume correlations begin at sampled times. Early-resume wall times and old realtime correlations may be inaccurate; sample uncertainty does not bound extrapolation or realtime clock changes. The original trace clock intervals are preserved.".into());
                        }
                    }
                    let decoder = decoder.as_ref().unwrap();
                    let writer = writer.as_mut().unwrap();
                    match record.kind {
                        RecordKind::Trace => {
                            trace_ordinal += 1;
                            ensure!(
                                summary.cpus.contains_key(&record.cpu),
                                "trace CPU missing from manifest"
                            );
                            let page_timestamp = decoder.endian.read(
                                &record.payload,
                                decoder.timestamp.offset,
                                decoder.timestamp.size,
                            )?;
                            ensure!(
                                page_timestamp == record.timestamp_ns,
                                "record/page base timestamps disagree"
                            );
                            if record.skip_bytes > 0 {
                                // A new independent capture can begin halfway through
                                // an mmap reader already consumed by the previous file.
                                // The raw prefix is still present for timestamp scanning.
                                // Only continuations within this export have a preceding
                                // acquired frontier that can be checked here.
                                if let Some((base, committed)) = last_pages.get(&record.cpu) {
                                    ensure!(
                                        *base == page_timestamp && *committed == record.skip_bytes,
                                        "mmap continuation does not follow previous CPU prefix"
                                    );
                                }
                            }
                            last_pages.insert(record.cpu, (page_timestamp, record.committed_bytes));
                            let mut page = decoder
                                .page(
                                    &record.payload,
                                    record.skip_bytes as usize,
                                    record.committed_bytes as usize,
                                )
                                .with_context(|| {
                                    format!(
                                        "CPU {} record {} raw page",
                                        record.cpu, record.sequence
                                    )
                                })?;
                            let kernel_lost = page.lost;
                            let recovered_loss = pending_recovery_loss.remove(&record.cpu);
                            if recovered_loss {
                                pending_export_recovery_loss.insert(record.cpu);
                                page.lost = true;
                                page.lost_events = None;
                            }
                            let cpu =
                                summary
                                    .cpus
                                    .entry(record.cpu)
                                    .or_insert_with(|| CpuSummary {
                                        lost_events: None,
                                        ..CpuSummary::default()
                                    });
                            let first_page = cpu.pages == 0;
                            cpu.pages += 1;
                            cpu.events += page.events.len() as u64;
                            if let Some(event) = page.events.first() {
                                cpu.first_timestamp_ns.get_or_insert(event.timestamp);
                            }
                            if let Some(event) = page.events.last() {
                                cpu.last_timestamp_ns = Some(event.timestamp);
                            }
                            if page.lost {
                                cpu.lost_pages += 1;
                            }
                            cpu.lost_events = match (cpu.lost_events, page.lost_events) {
                                (None, Some(count)) if first_page => Some(count),
                                (Some(old), Some(count)) => {
                                    Some(old.checked_add(count).context("loss count overflow")?)
                                }
                                _ => None,
                            };
                            // CPU summaries describe raw records, whereas summary.events
                            // counts events actually emitted into the Perfetto stream.
                            let cutoff = leading_overrun_records.get(&record.cpu).copied();
                            if cutoff.is_some_and(|cutoff| trace_ordinal < cutoff) {
                                cpu.omitted_pages += 1;
                                cpu.leading_loss_pages += u64::from(kernel_lost);
                                cpu.omitted_events += page.events.len() as u64;
                                continue;
                            }
                            let suppressed = kernel_lost && cutoff == Some(trace_ordinal);
                            if suppressed {
                                cpu.leading_loss_pages += 1;
                            }
                            if kernel_lost && !suppressed {
                                pending_kernel_loss.insert(record.cpu);
                            }
                            page.lost = if options.raw {
                                kernel_lost || recovered_loss
                            } else {
                                pending_kernel_loss.contains(&record.cpu)
                                    || pending_export_recovery_loss.contains(&record.cpu)
                            };
                            if !page.events.is_empty() {
                                pending_kernel_loss.remove(&record.cpu);
                                pending_export_recovery_loss.remove(&record.cpu);
                            }
                            let mut bundle = Proto::default();
                            bundle.uint(1, record.cpu as u64);
                            bundle.uint(3, u64::from(page.lost));
                            if decoder.preserve_monotonic {
                                // FtraceClock has no MONOTONIC selector. GLOBAL(2)
                                // selects sequence-local clock64 in Perfetto's
                                // FtraceTokenizer::HandleFtraceClockSnapshot. Our
                                // native snapshots map clock64 identically to mono3.
                                // This selector is an encoding accommodation, not
                                // a claim that tracefs used its "global" clock.
                                // See protos/perfetto/trace/ftrace/ftrace_event_bundle.proto
                                // and src/trace_processor/importers/ftrace/ftrace_tokenizer.cc.
                                // Omit per-bundle boot pairs: these sampled mappings
                                // must never perturb original event intervals.
                                bundle.uint(5, 2);
                            }
                            for event in &page.events {
                                if let Some(encoded) = encode_event(decoder, event, &mut summary)? {
                                    bundle.message(2, encoded);
                                    summary.events += 1;
                                    if let Some(syscall) = encode_syscall(decoder, event)? {
                                        bundle.message(2, syscall);
                                    }
                                }
                            }
                            write_packet(writer, packet(1, bundle, None))?;
                        }
                        RecordKind::Trigger => {
                            let value: serde_json::Value = serde_json::from_slice(&record.payload)?;
                            ensure!(
                                value["schema_version"].as_u64() == Some(1),
                                "unsupported trigger schema"
                            );
                            let timestamp: u64 = value["timestamp_ns"]
                                .as_str()
                                .context("trigger lacks decimal-string timestamp")?
                                .parse()?;
                            let timestamp = if samples.is_empty() {
                                decoder.clocks.monotonic_to_boot(timestamp)?
                            } else {
                                timestamp
                            };
                            write_packet(
                                writer,
                                trigger_marker(
                                    timestamp,
                                    &value,
                                    if samples.is_empty() { 6 } else { 3 },
                                )?,
                            )?;
                            summary.triggers += 1;
                        }
                        RecordKind::Stats => {
                            let value: serde_json::Value = serde_json::from_slice(&record.payload)
                                .context("invalid accounting JSON")?;
                            write_packet(
                                writer,
                                accounting_marker(
                                    if samples.is_empty() {
                                        decoder.clocks.monotonic_to_boot(record.timestamp_ns)?
                                    } else {
                                        record.timestamp_ns
                                    },
                                    &value,
                                    if samples.is_empty() { 6 } else { 3 },
                                )?,
                            )?;
                            summary.last_accounting = Some(value);
                        }
                        RecordKind::End => {
                            let end: serde_json::Value = serde_json::from_slice(&record.payload)
                                .context("invalid end JSON")?;
                            ensure!(
                                end["schema_version"].as_u64() == Some(1),
                                "unsupported end metadata version"
                            );
                            summary.completed = end["completed"].as_bool() == Some(true);
                            summary.capture_end = Some(end);
                            saw_end = true;
                        }
                        RecordKind::ClockSnapshot => {} // Emitted from the first pass.
                        _ => unreachable!(),
                    }
                }
            }
        }
        summary.recovered_tail = reader.recovered_tail;
        summary.completed &= summary.recovered_tail.is_none();
    }
    for cpu in summary.cpus.values_mut() {
        if let (Some(first), Some(last)) = (cpu.first_timestamp_ns, cpu.last_timestamp_ns) {
            cpu.recovered_duration_ns = last.checked_sub(first);
        }
    }
    if !summary.completed {
        summary.warnings.push(
            "Capture has no valid completion record; coverage/durability is incomplete.".into(),
        );
    }
    if !summary.unknown_event_ids.is_empty() {
        summary.warnings.push(
            "Events without preserved formats were omitted; counts are reported by ID.".into(),
        );
    }
    if let Some(writer) = writer.as_mut() {
        writer.flush()?;
    } else {
        bail!("capture contains no trace/trigger/accounting records");
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use retroft_format::{metadata_file_payload, write_file_header, write_record, RecordHeader};

    const HEADER_EVENT: &str = include_str!("../../../tests/fixtures/offline/header_event");
    const PAGE64: &[u8] = include_bytes!("../../../tests/fixtures/offline/little64.page");
    const PAGE32: &[u8] = include_bytes!("../../../tests/fixtures/offline/big32.page");

    #[test]
    fn summary_json_preserves_decimal_strings_and_unknown_values() -> Result<()> {
        let summary = ExportSummary {
            maximum_clock_sample_uncertainty_ns: Some(u64::MAX),
            cpus: BTreeMap::from([
                (
                    0,
                    CpuSummary {
                        first_timestamp_ns: Some(u64::MAX - 9),
                        last_timestamp_ns: Some(u64::MAX),
                        recovered_duration_ns: Some(9),
                        lost_events: Some(u64::MAX),
                        ..CpuSummary::default()
                    },
                ),
                (1, CpuSummary::default()),
            ]),
            ..ExportSummary::default()
        };
        let json = serde_json::to_value(&summary)?;
        assert_eq!(
            json["maximum_clock_sample_uncertainty_ns"],
            "18446744073709551615"
        );
        assert_eq!(
            json["cpus"]["0"]["first_timestamp_ns"],
            "18446744073709551606"
        );
        assert_eq!(
            json["cpus"]["0"]["last_timestamp_ns"],
            "18446744073709551615"
        );
        assert_eq!(json["cpus"]["0"]["recovered_duration_ns"], "9");
        assert_eq!(json["cpus"]["0"]["lost_events"], "18446744073709551615");
        assert_eq!(json["cpus"]["0"]["events"], 0);
        for field in [
            "first_timestamp_ns",
            "last_timestamp_ns",
            "recovered_duration_ns",
            "lost_events",
        ] {
            assert!(json["cpus"]["1"][field].is_null());
        }
        assert!(serde_json::to_value(ExportSummary::default())?
            ["maximum_clock_sample_uncertainty_ns"]
            .is_null());
        Ok(())
    }

    fn metadata(big32: bool) -> (Manifest, BTreeMap<String, Vec<u8>>) {
        let manifest = serde_json::json!({
            "schema_version":1,"endianness":if big32 {"big"} else {"little"},
            "word_size":if big32 {4} else {8},"subbuffer_size":4096,"trace_clock":"mono",
            "cpus":[0,1], "header_page_ref":"events/header_page", "header_event_ref":"events/header_event",
            "clock_monotonic_ns":"100", "clock_boottime_ns":"100",
            "events":[{"id":100,"system":"sched","name":"sched_switch","format_ref":"sched_switch"},
                       {"id":101,"system":"sched","name":"sched_waking","format_ref":"sched_waking"}],
            "initial_tasks":[{"tid":42,"tgid":42,"comm":"worker"},{"tid":43,"tgid":42,"comm":"worker-child"}]
        });
        let header = if big32 {
            include_str!("../../../tests/fixtures/offline/header_page_32")
        } else {
            include_str!("../../../tests/fixtures/offline/header_page_64")
        };
        let switch = if big32 {
            include_str!("../../../tests/fixtures/offline/sched_switch_32.format")
        } else {
            include_str!("../../../tests/fixtures/offline/sched_switch.format")
        };
        let files = [
            ("events/header_page", header),
            ("events/header_event", HEADER_EVENT),
            ("sched_switch", switch),
            (
                "sched_waking",
                include_str!("../../../tests/fixtures/offline/sched_waking.format"),
            ),
        ]
        .into_iter()
        .map(|(path, text)| (path.to_owned(), text.as_bytes().to_vec()))
        .collect();
        (serde_json::from_value(manifest).unwrap(), files)
    }

    fn decoder(big32: bool) -> Decoder {
        let (manifest, files) = metadata(big32);
        Decoder::new(&manifest, &files).unwrap()
    }

    #[test]
    fn decodes_two_target_abis_and_timestamp_controls() -> Result<()> {
        for (big32, bytes, committed, length) in [(false, PAGE64, 276, 64), (true, PAGE32, 264, 60)]
        {
            let decoder = decoder(big32);
            let page = decoder.page(bytes, 0, committed)?;
            assert_eq!(
                page.events.iter().map(|e| e.timestamp).collect::<Vec<_>>(),
                [1010, 134218748, 268437467, 268437474]
            );
            assert_eq!(
                page.events.iter().map(|e| e.id).collect::<Vec<_>>(),
                [100, 101, 100, 100]
            );
            assert_eq!(page.events[0].payload.len(), length);
            assert!(page.lost);
            assert_eq!(page.lost_events, Some(9));
            let state = &decoder.formats[&100].fields["prev_state"];
            assert!(matches!(
                state.value(&page.events[2].payload, decoder.endian)?,
                Value::Signed(-1)
            ));
        }
        Ok(())
    }

    #[test]
    fn mmap_authoritative_prefix_does_not_export_duplicates_or_repeat_losses() -> Result<()> {
        let decoder = decoder(false);
        let first = decoder.page(PAGE64, 0, 68)?;
        assert_eq!(first.events.len(), 1);
        assert!(first.lost);
        assert_eq!(first.lost_events, None);
        let rest = decoder.page(PAGE64, 68, 276)?;
        assert_eq!(rest.events.len(), 3);
        assert_eq!(rest.events[0].timestamp, 134218748);
        assert!(!rest.lost);
        assert_eq!(rest.lost_events, Some(0));
        assert!(decoder.page(PAGE64, 4, 276).is_err());
        assert!(decoder.page(PAGE64, 0, 67).is_err());
        assert!(decoder.page(PAGE64, 0, 280).is_err());
        Ok(())
    }

    #[test]
    fn rejects_malformed_lengths_and_out_of_bounds_fields() -> Result<()> {
        let decoder = decoder(false);
        assert!(decoder.page(&PAGE64[..4095], 0, 276).is_err());
        let mut bad = PAGE64.to_vec();
        // First record is extended-size with an impossible u32 payload length.
        bad[16..20].copy_from_slice(&0u32.to_le_bytes());
        bad[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decoder.page(&bad, 0, 276).is_err());
        let field = Field {
            name: "filename".into(),
            declaration: "__data_loc char[] filename".into(),
            offset: 0,
            size: 4,
            signed: false,
        };
        assert!(field
            .value(&0xffff_ffffu32.to_le_bytes(), Endian::Little)
            .is_err());
        Ok(())
    }

    #[test]
    fn parses_dynamic_relative_strings_and_signed_scalars() -> Result<()> {
        let fields = fields("field:__rel_loc char[] filename; offset:4; size:4; signed:0;\nfield:short delta; offset:0; size:2; signed:1;")?;
        let mut bytes = vec![0; 20];
        bytes[..2].copy_from_slice(&(-17i16).to_be_bytes());
        // Relative address = field offset 4 + locator size 4 + displacement 3.
        bytes[4..8].copy_from_slice(&((5u32 << 16) | 3).to_be_bytes());
        bytes[11..16].copy_from_slice(b"exec\0");
        assert!(
            matches!(fields["filename"].value(&bytes,Endian::Big)?,Value::String(v) if v=="exec")
        );
        assert!(matches!(
            fields["delta"].value(&bytes, Endian::Big)?,
            Value::Signed(-17)
        ));
        Ok(())
    }

    #[test]
    fn restores_absolute_timestamp_high_bits_and_rollover() -> Result<()> {
        let decoder = decoder(false);
        let mut page = PAGE64.to_vec();
        // Start just before a 59-bit epoch rolls over. The absolute control's
        // low 59 bits are 7; restoring epoch 1 gives an older value, so add epoch.
        let base = (2u64 << 59) - 10;
        page[..8].copy_from_slice(&base.to_le_bytes());
        page[8..16].copy_from_slice(&76u64.to_le_bytes());
        page[16..20].copy_from_slice(&((7u32 << 5) | 31).to_le_bytes());
        page[20..24].copy_from_slice(&0u32.to_le_bytes());
        page[24..28].copy_from_slice(&16u32.to_le_bytes());
        page[28..92].copy_from_slice(&PAGE64[20..84]);
        let result = decoder.page(&page, 0, 76)?;
        assert_eq!(result.events[0].timestamp, (2u64 << 59) + 7);
        Ok(())
    }

    #[test]
    fn rejects_unknown_required_metadata_abis_and_mismatched_ids() {
        let (mut manifest, mut files) = metadata(false);
        manifest.schema_version = 2;
        assert!(Decoder::new(&manifest, &files).is_err());
        manifest.schema_version = 1;
        manifest.word_size = 4;
        assert!(Decoder::new(&manifest, &files).is_err());
        manifest.word_size = 8;
        manifest.events[0].id = 999;
        assert!(Decoder::new(&manifest, &files).is_err());
        manifest.events[0].id = 100;
        files.insert(
            "events/header_event".into(),
            b"type_len : 6 bits\ntime_delta : 26 bits\n".to_vec(),
        );
        assert!(Decoder::new(&manifest, &files).is_err());
    }

    #[test]
    fn exports_dynamic_lifecycle_irq_and_generic_fields_and_reports_unknown_ids() -> Result<()> {
        #[derive(Clone, PartialEq, Message)]
        struct RawEvent {
            #[prost(bytes, tag = "237")]
            exec: Vec<u8>,
            #[prost(bytes, tag = "36")]
            irq: Vec<u8>,
            #[prost(bytes, tag = "327")]
            generic: Vec<u8>,
        }
        #[derive(Clone, PartialEq, Message)]
        struct Exec {
            #[prost(string, tag = "1")]
            filename: String,
            #[prost(int32, tag = "2")]
            pid: i32,
            #[prost(int32, tag = "3")]
            old_pid: i32,
        }
        #[derive(Clone, PartialEq, Message)]
        struct Irq {
            #[prost(int32, tag = "1")]
            irq: i32,
            #[prost(string, tag = "2")]
            name: String,
        }
        #[derive(Clone, PartialEq, Message)]
        struct Generic {
            #[prost(string, tag = "1")]
            name: String,
            #[prost(message, repeated, tag = "2")]
            fields: Vec<GenericField>,
        }
        #[derive(Clone, PartialEq, Message)]
        struct GenericField {
            #[prost(string, tag = "1")]
            name: String,
            #[prost(int64, optional, tag = "4")]
            signed: Option<i64>,
            #[prost(uint64, optional, tag = "5")]
            unsigned: Option<u64>,
        }
        let mut decoder = decoder(false);
        let common = "field:unsigned short common_type; offset:0; size:2; signed:0;\nfield:int common_pid; offset:4; size:4; signed:1;\n";
        let examples = [
            (102,"sched","sched_process_exec","field:__data_loc char[] filename; offset:8; size:4; signed:0;\nfield:pid_t pid; offset:12; size:4; signed:1;\nfield:pid_t old_pid; offset:16; size:4; signed:1;", {
                let mut bytes = vec![0; 28]; bytes[8..12].copy_from_slice(&((8u32<<16)|20).to_le_bytes()); bytes[12..16].copy_from_slice(&42u32.to_le_bytes()); bytes[16..20].copy_from_slice(&41u32.to_le_bytes()); bytes[20..].copy_from_slice(b"/bin/cc\0"); bytes
            }),
            (103,"irq","irq_handler_entry","field:int irq; offset:8; size:4; signed:1;\nfield:__data_loc char[] name; offset:12; size:4; signed:0;", {
                let mut bytes = vec![0; 24]; bytes[8..12].copy_from_slice(&17u32.to_le_bytes()); bytes[12..16].copy_from_slice(&((8u32<<16)|16).to_le_bytes()); bytes[16..].copy_from_slice(b"network\0"); bytes
            }),
            (104,"lock","contention_begin","field:long delta; offset:8; size:8; signed:1;\nfield:unsigned long lock_addr; offset:16; size:8; signed:0;", {
                let mut bytes = vec![0;24]; bytes[8..16].copy_from_slice(&(-5i64).to_le_bytes()); bytes[16..24].copy_from_slice(&0xfedcba9876543210u64.to_le_bytes()); bytes
            }),
        ];
        let mut summary = ExportSummary::default();
        for (id, system, name, extra, mut payload) in examples {
            payload[..2].copy_from_slice(&(id as u16).to_le_bytes());
            payload[4..8].copy_from_slice(&42u32.to_le_bytes());
            decoder.formats.insert(
                id,
                EventFormat {
                    system: system.into(),
                    name: name.into(),
                    fields: fields(&(common.to_owned() + extra))?,
                },
            );
            let encoded = encode_event(
                &decoder,
                &DecodedEvent {
                    timestamp: 1234,
                    id,
                    payload,
                },
                &mut summary,
            )?
            .unwrap();
            let raw = RawEvent::decode(encoded.0.as_slice())?;
            match id {
                102 => {
                    let exec = Exec::decode(raw.exec.as_slice())?;
                    assert_eq!(exec.filename, "/bin/cc");
                    assert_eq!(exec.pid, 42);
                    assert_eq!(exec.old_pid, 41);
                }
                103 => {
                    let irq = Irq::decode(raw.irq.as_slice())?;
                    assert_eq!(irq.irq, 17);
                    assert_eq!(irq.name, "network");
                }
                104 => {
                    let generic = Generic::decode(raw.generic.as_slice())?;
                    assert_eq!(generic.name, "lock:contention_begin");
                    assert_eq!(generic.fields[0].signed, Some(-5));
                    assert_eq!(generic.fields[1].unsigned, Some(0xfedcba9876543210));
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(summary.generic_events, 1);
        assert!(encode_event(
            &decoder,
            &DecodedEvent {
                timestamp: 1,
                id: 999,
                payload: vec![0; 8]
            },
            &mut summary
        )?
        .is_none());
        assert_eq!(summary.unknown_event_ids[&999], 1);
        Ok(())
    }

    // A separately derived schema decodes the wire output. The exporter never
    // calls these generated readers, so mismatched tags/types fail assertions.
    #[derive(Clone, PartialEq, Message)]
    struct Trace {
        #[prost(message, repeated, tag = "1")]
        packets: Vec<Packet>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Packet {
        #[prost(message, optional, tag = "1")]
        ftrace: Option<Bundle>,
        #[prost(message, optional, tag = "5")]
        chrome: Option<ChromeMetadataBundle>,
        #[prost(message, optional, tag = "6")]
        clock_snapshot: Option<Snapshot>,
        #[prost(uint64, optional, tag = "8")]
        timestamp: Option<u64>,
        #[prost(uint32, optional, tag = "10")]
        sequence: Option<u32>,
        #[prost(message, optional, tag = "11")]
        track: Option<Track>,
        #[prost(uint32, optional, tag = "58")]
        timestamp_clock_id: Option<u32>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct ChromeMetadataBundle {
        #[prost(message, repeated, tag = "2")]
        metadata: Vec<ChromeMetadataEntry>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct ChromeMetadataEntry {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(string, tag = "2")]
        value: String,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Snapshot {
        #[prost(message, repeated, tag = "1")]
        clocks: Vec<Clock>,
        #[prost(uint32, tag = "2")]
        primary_trace_clock: u32,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Clock {
        #[prost(uint32, tag = "1")]
        id: u32,
        #[prost(uint64, tag = "2")]
        timestamp: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Bundle {
        #[prost(uint32, required, tag = "1")]
        cpu: u32,
        #[prost(message, repeated, tag = "2")]
        events: Vec<Event>,
        #[prost(bool, tag = "3")]
        lost: bool,
        #[prost(uint32, optional, tag = "5")]
        clock: Option<u32>,
        #[prost(uint64, optional, tag = "10")]
        previous_bundle_end_timestamp: Option<u64>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Event {
        #[prost(uint32, required, tag = "2")]
        pid: u32,
        #[prost(uint64, tag = "1")]
        timestamp: u64,
        #[prost(uint32, optional, tag = "5")]
        common_flags: Option<u32>,
        #[prost(message, optional, tag = "4")]
        switch: Option<Switch>,
        #[prost(message, optional, tag = "20")]
        waking: Option<Waking>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Switch {
        #[prost(int32, tag = "2")]
        prev_pid: i32,
        #[prost(int64, tag = "4")]
        prev_state: i64,
        #[prost(string, tag = "5")]
        next_comm: String,
        #[prost(int32, tag = "6")]
        next_pid: i32,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Waking {
        #[prost(int32, tag = "2")]
        pid: i32,
        #[prost(int32, tag = "5")]
        target_cpu: i32,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Track {
        #[prost(message, repeated, tag = "4")]
        annotations: Vec<Annotation>,
        #[prost(int32, tag = "9")]
        kind: i32,
        #[prost(uint64, tag = "11")]
        uuid: u64,
        #[prost(string, tag = "23")]
        name: String,
    }

    #[derive(Clone, PartialEq, Message)]
    struct Annotation {
        #[prost(string, tag = "10")]
        name: String,
        #[prost(string, optional, tag = "6")]
        text: Option<String>,
        #[prost(uint64, optional, tag = "3")]
        number: Option<u64>,
        #[prost(message, repeated, tag = "11")]
        fields: Vec<Annotation>,
        #[prost(message, repeated, tag = "12")]
        values: Vec<Annotation>,
    }

    #[test]
    fn source_paths_are_embedded_in_input_order() -> Result<()> {
        let paths = [
            Path::new("captures/quoted\" name.retroft"),
            Path::new("/tmp/second.retroft"),
        ];
        let packet = Packet::decode(source_capture_metadata(&paths)?.0.as_slice())?;
        let entries = packet.chrome.unwrap().metadata;
        assert_eq!(entries[0].name, "retroft.source_capture_paths");
        let decoded: Vec<String> = serde_json::from_str(&entries[0].value)?;
        assert_eq!(
            decoded,
            paths
                .iter()
                .map(|p| std::path::absolute(p)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned())
                .collect::<Vec<_>>()
        );

        let directory = tempfile::tempdir()?;
        let input = directory.path().join("source.retroft");
        let output = directory.path().join("trace.pftrace");
        std::fs::write(&input, fixture_capture(false, true)?)?;
        export(&input, &output)?;
        let trace = Trace::decode(std::fs::read(&output)?.as_slice())?;
        let metadata = &trace.packets[0].chrome.as_ref().unwrap().metadata[0];
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&metadata.value)?,
            vec![input.to_string_lossy()]
        );
        if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
            let result = std::process::Command::new(processor)
                .arg(&output)
                .arg("-Q")
                .arg(
                    "SELECT str_value FROM metadata WHERE name = 'cr-retroft.source_capture_paths'",
                )
                .output()?;
            ensure!(
                result.status.success(),
                "Perfetto failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert!(String::from_utf8(result.stdout)?.contains(input.to_str().unwrap()));
        }
        Ok(())
    }

    #[test]
    fn named_syscalls_export_native_slices_and_preserve_fields() -> Result<()> {
        #[derive(Clone, PartialEq, Message)]
        struct Native {
            #[prost(message, optional, tag = "329")]
            enter: Option<Enter>,
            #[prost(message, optional, tag = "330")]
            exit: Option<Exit>,
        }
        #[derive(Clone, PartialEq, Message)]
        struct Enter {
            #[prost(int64, tag = "1")]
            id: i64,
            #[prost(uint64, repeated, tag = "2")]
            args: Vec<u64>,
        }
        #[derive(Clone, PartialEq, Message)]
        struct Exit {
            #[prost(int64, tag = "2")]
            ret: i64,
        }
        // read(2) has different numbers on these ABIs. ARM also exercises
        // big-endian 32-bit fields and sign extension of a negative return.
        for (architecture, nr, big32) in [
            ("x86_64", 0u64, false),
            ("aarch64", 63, false),
            ("arm", 3, true),
        ] {
            let (mut manifest, files) = metadata(big32);
            manifest.architecture = Some(architecture.into());
            let mut decoder = Decoder::new(&manifest, &files)?;
            let word = decoder.word_size;
            let common = "field:unsigned short common_type; offset:0; size:2; signed:0;\nfield:int common_pid; offset:4; size:4; signed:1;\nfield:int __syscall_nr; offset:8; size:4; signed:1;\n";
            for (id, name, extra) in [
                (200, "sys_enter_read", format!("field:unsigned int fd; offset:16; size:{word}; signed:0;\nfield:char * buf; offset:{}; size:{word}; signed:0;\nfield:size_t count; offset:{}; size:{word}; signed:0;", 16+word, 16+2*word)),
                (201, "sys_exit_read", format!("field:long ret; offset:16; size:{word}; signed:1;")),
            ] {
                decoder.formats.insert(id, EventFormat {system: "syscalls".into(), name: name.into(), fields: fields(&(common.to_owned()+&extra))?});
            }
            let mut bytes = Vec::new();
            write_packet(
                &mut bytes,
                kernel_info("6.1.0", syscall_machine(Some(architecture))),
            )?;
            let mut summary = ExportSummary::default();
            // An unmatched leading exit must not invent a completed call;
            // an unmatched trailing entry must remain an unfinished slice.
            for (timestamp, id) in [(100, 201), (200, 200), (350, 201), (400, 200)] {
                let mut payload = vec![0; 16 + 3 * word];
                let mut put = |offset: usize, size: usize, value: u64| {
                    let raw = if big32 {
                        value.to_be_bytes()
                    } else {
                        value.to_le_bytes()
                    };
                    payload[offset..offset + size].copy_from_slice(if big32 {
                        &raw[8 - size..]
                    } else {
                        &raw[..size]
                    });
                };
                put(4, 4, 42);
                put(8, 4, nr);
                if id == 200 {
                    put(16, word, 7);
                    put(16 + word, word, 0x1234);
                    put(16 + 2 * word, word, 99);
                } else {
                    put(16, word, (-9i64) as u64);
                }
                let event = DecodedEvent {
                    timestamp,
                    id,
                    payload,
                };
                let native = encode_syscall(&decoder, &event)?.unwrap();
                let decoded = Native::decode(native.0.as_slice())?;
                if id == 200 {
                    let enter = decoded.enter.unwrap();
                    assert_eq!(enter.id, nr as i64);
                    assert_eq!(enter.args, vec![7, 0x1234, 99]);
                } else {
                    assert_eq!(decoded.exit.unwrap().ret, -9);
                }
                let mut bundle = Proto::default();
                bundle.uint(1, if timestamp == 350 { 1 } else { 0 });
                bundle.message(2, encode_event(&decoder, &event, &mut summary)?.unwrap());
                bundle.message(2, native);
                write_packet(&mut bytes, packet(1, bundle, None))?;
                decoder.native_syscalls = false;
                assert!(encode_syscall(&decoder, &event)?.is_none());
                decoder.native_syscalls = true;
            }
            assert_eq!(summary.generic_events, 4);
            if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
                let directory = tempfile::tempdir()?;
                let path = directory.path().join("syscalls.pftrace");
                std::fs::write(&path, bytes)?;
                let result = std::process::Command::new(processor).arg(path).arg("-Q").arg(
                    "SELECT name, dur, EXTRACT_ARG(arg_set_id, 'args[0]') AS fd, EXTRACT_ARG(arg_set_id, 'ret') AS ret FROM slice ORDER BY ts"
                ).output()?;
                ensure!(
                    result.status.success(),
                    "Perfetto failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                let output = String::from_utf8(result.stdout)?;
                assert!(
                    output.contains("\"sys_read\",150,7,-9"),
                    "{architecture}: {output}"
                );
                assert!(
                    output.contains("\"sys_read\",-1,7,"),
                    "{architecture}: {output}"
                );
                assert_eq!(output.lines().count(), 3, "{output}");
            }
        }
        Ok(())
    }

    #[test]
    fn recorded_kernel_release_selects_preemption_and_idle_states() -> Result<()> {
        for (release, expected) in [
            ("4.4.0", "\"end_state\"\n\"K\"\n\"W\""),
            ("6.1.0", "\"end_state\"\n\"I\"\n\"R+\""),
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("kernel.pftrace");
            let mut bytes = Vec::new();
            write_packet(&mut bytes, kernel_info(release, None))?;
            let mut bundle = Proto::default();
            bundle.uint(1, 0);
            for (timestamp, prev, next, state) in
                [(100, 0, 42, 0), (200, 42, 43, 256), (300, 43, 42, 128)]
            {
                let mut switch = Proto::default();
                switch.string(1, "previous");
                switch.uint(2, prev);
                switch.uint(3, 120);
                switch.uint(4, state);
                switch.string(5, "next");
                switch.uint(6, next);
                switch.uint(7, 120);
                let mut event = Proto::default();
                event.uint(1, timestamp);
                event.uint(2, prev);
                event.message(4, switch);
                bundle.message(2, event);
            }
            write_packet(&mut bytes, packet(1, bundle, None))?;
            std::fs::write(&path, bytes)?;
            if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
                let result = std::process::Command::new(processor)
                    .arg(path)
                    .arg("-Q")
                    .arg("SELECT DISTINCT end_state FROM sched WHERE dur > 0 ORDER BY end_state")
                    .output()?;
                ensure!(
                    result.status.success(),
                    "Perfetto failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                assert_eq!(String::from_utf8(result.stdout)?.trim(), expected);
            }
        }
        Ok(())
    }

    #[test]
    fn accounting_uses_short_names_and_nested_arguments() -> Result<()> {
        let value = serde_json::json!({
            "schema_version": 1, "phase": "end",
            "cpus": [{"cpu": 7, "entries": "18446744073709551615", "overrun": null,
                      "raw": "entries: redundant original text"}],
            "cpu_boundaries": [{"cpu": 7, "completion": "grace_timeout"}],
            "staging_exhaustion_observations": "3"
        });
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("accounting.pftrace");
        let mut bytes = Vec::new();
        write_packet(&mut bytes, track_descriptor())?;
        for phase in ["start", "end"] {
            let mut payload = value.clone();
            payload["phase"] = phase.into();
            write_packet(&mut bytes, accounting_marker(1000, &payload, 6)?)?;
        }
        let trace = Trace::decode(bytes.as_slice())?;
        let markers: Vec<_> = trace
            .packets
            .iter()
            .filter_map(|p| p.track.as_ref())
            .collect();
        assert_eq!(markers[0].name, "Capture start");
        assert_eq!(markers[1].name, "Capture end");
        let cpu = &markers[1]
            .annotations
            .iter()
            .find(|a| a.name == "cpus")
            .unwrap()
            .values[0];
        assert!(cpu.fields.iter().all(|field| field.name != "raw"));
        assert_eq!(
            cpu.fields
                .iter()
                .find(|a| a.name == "entries")
                .unwrap()
                .text
                .as_deref(),
            Some("18446744073709551615")
        );
        std::fs::write(&path, bytes)?;
        if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
            let result = std::process::Command::new(processor).arg(&path).arg("-Q").arg(
                "SELECT a.key,a.display_value FROM slice s JOIN args a USING(arg_set_id) WHERE s.name = 'Capture end' AND a.key IN ('debug.cpus[0].entries','debug.cpu_boundaries[0].completion') ORDER BY a.key"
            ).output()?;
            ensure!(
                result.status.success(),
                "Perfetto failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert_eq!(
                String::from_utf8(result.stdout)?.trim(),
                concat!(
                    "\"key\",\"display_value\"\n",
                    "\"debug.cpu_boundaries[0].completion\",\"grace_timeout\"\n",
                    "\"debug.cpus[0].entries\",\"18446744073709551615\""
                )
            );
        }
        Ok(())
    }

    #[test]
    fn leading_overruns_cleanup_uses_fixed_all_cpu_boundary_and_raw_preserves_history() -> Result<()>
    {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::write(&source, fixture_capture(false, true)?)?;
        for empty_loss in [false, true] {
            let mut reader = CaptureReader::open(&source)?;
            let mut bytes = retroft_format::FILE_HEADER.to_vec();
            let mut sequence = 1;
            while let Some(record) = reader.next_record()? {
                if record.kind == RecordKind::Trace {
                    // CPU 1 begins later; CPU 0 has several early overruns and
                    // another after the fixed boundary. CPU 2 keeps old history.
                    for (cpu, base, loss, empty) in [
                        (0, 1000u64, false, false),
                        (0, 2000, true, false),
                        (0, 3000, true, empty_loss),
                        (0, 4000, false, false),
                        (1, 5000, false, false),
                        (2, 500, false, false),
                        (0, 10000, true, false),
                    ] {
                        let committed = if empty { 0 } else { 68 };
                        let mut page = PAGE64.to_vec();
                        page[..8].copy_from_slice(&base.to_le_bytes());
                        page[8..16].copy_from_slice(
                            &(committed as u64 | if loss { 1 << 31 } else { 0 }).to_le_bytes(),
                        );
                        let mut header = RecordHeader::new(RecordKind::Trace, sequence, base);
                        header.cpu = cpu;
                        header.committed_bytes = committed;
                        write_record(&mut bytes, header, &page)?;
                        sequence += 1;
                    }
                } else {
                    let mut payload = record.payload;
                    if record.kind == RecordKind::Metadata {
                        let mut value: serde_json::Value = serde_json::from_slice(&payload)?;
                        value["cpus"] = serde_json::json!([0, 1, 2]);
                        payload = serde_json::to_vec(&value)?;
                    }
                    write_record(
                        &mut bytes,
                        RecordHeader::new(record.kind, sequence, record.timestamp_ns),
                        &payload,
                    )?;
                    sequence += 1;
                }
            }
            let input = directory.path().join(format!("capture-{empty_loss}"));
            std::fs::write(&input, bytes)?;
            for raw in [false, true] {
                let output = directory.path().join(format!("trace-{empty_loss}-{raw}"));
                let summary =
                    export_captures_with_options(&[&input], &output, ExportOptions { raw })?;
                assert_eq!(summary.cpus[&0].pages, 5);
                assert_eq!(summary.cpus[&0].lost_pages, 3);
                assert_eq!(summary.cpus[&0].omitted_pages, if raw { 0 } else { 2 });
                assert_eq!(summary.cpus[&0].omitted_events, if raw { 0 } else { 2 });
                assert_eq!(summary.cpus[&0].leading_loss_pages, if raw { 0 } else { 2 });
                assert_eq!(summary.cpus[&2].omitted_events, 0);
                assert_eq!(
                    summary.events,
                    (if empty_loss { 6 } else { 7 }) - if raw { 0 } else { 2 }
                );
                let trace = Trace::decode(std::fs::read(output)?.as_slice())?;
                let bundles = trace
                    .packets
                    .iter()
                    .filter_map(|packet| packet.ftrace.as_ref())
                    .collect::<Vec<_>>();
                assert_eq!(
                    bundles
                        .iter()
                        .filter(|bundle| bundle.previous_bundle_end_timestamp.is_some())
                        .count(),
                    if raw { 0 } else { 3 }
                );
                assert!(bundles.iter().any(|bundle| bundle.lost
                    && bundle
                        .events
                        .first()
                        .is_some_and(|event| event.timestamp == 10510)));
                if !raw {
                    assert!(!bundles.iter().any(|bundle| bundle.lost
                        && bundle
                            .events
                            .first()
                            .is_some_and(|event| event.timestamp < 10510)));
                    assert!(bundles.iter().any(|bundle| bundle
                        .events
                        .first()
                        .is_some_and(|event| event.timestamp == 1010)));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn empty_loss_mmap_continuation_and_missing_cpu_keep_conservative_coverage() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::write(&source, fixture_capture(false, true)?)?;
        for missing_cpu in [false, true] {
            for loss_base in [2000u64, 10000] {
                let mut reader = CaptureReader::open(&source)?;
                let mut bytes = retroft_format::FILE_HEADER.to_vec();
                let mut sequence = 1;
                while let Some(record) = reader.next_record()? {
                    if record.kind == RecordKind::Trace {
                        for (cpu, base, committed, skip, loss) in [
                            (0, 1000, 68, 0, false),
                            (0, loss_base, 8, 0, true),
                            (0, loss_base, 76, 8, true),
                            (1, 5000, 68, 0, false),
                        ] {
                            let mut page = PAGE64.to_vec();
                            page[..8].copy_from_slice(&base.to_le_bytes());
                            page[8..16].copy_from_slice(
                                &(committed as u64 | if loss { 1 << 31 } else { 0 }).to_le_bytes(),
                            );
                            if loss {
                                // An initial TIME_EXTEND control contains no event.
                                // The next acquisition grows this exact mmap page.
                                page.copy_within(16..84, 24);
                                page[16..20].copy_from_slice(&30u32.to_le_bytes());
                                page[20..24].fill(0);
                            }
                            let mut header = RecordHeader::new(RecordKind::Trace, sequence, base);
                            header.cpu = cpu;
                            header.committed_bytes = committed;
                            header.skip_bytes = skip;
                            write_record(&mut bytes, header, &page)?;
                            sequence += 1;
                        }
                    } else {
                        let mut payload = record.payload;
                        if record.kind == RecordKind::Metadata {
                            let mut value: serde_json::Value = serde_json::from_slice(&payload)?;
                            value["cpus"] = if missing_cpu {
                                serde_json::json!([0, 1, 2])
                            } else {
                                serde_json::json!([0, 1])
                            };
                            payload = serde_json::to_vec(&value)?;
                        }
                        write_record(
                            &mut bytes,
                            RecordHeader::new(record.kind, sequence, record.timestamp_ns),
                            &payload,
                        )?;
                        sequence += 1;
                    }
                }
                let input = directory
                    .path()
                    .join(format!("capture-{missing_cpu}-{loss_base}"));
                let output = directory
                    .path()
                    .join(format!("trace-{missing_cpu}-{loss_base}"));
                std::fs::write(&input, bytes)?;
                let summary = export(&input, &output)?;
                let cleanup = !missing_cpu && loss_base == 2000;
                assert_eq!(summary.events, if cleanup { 2 } else { 3 });
                assert_eq!(summary.cpus[&0].lost_pages, 1);
                assert_eq!(summary.cpus[&0].omitted_pages, u64::from(cleanup));
                assert_eq!(summary.cpus[&0].leading_loss_pages, u64::from(cleanup));
                if missing_cpu {
                    assert!(summary
                        .warnings
                        .iter()
                        .any(|warning| warning.contains("CPU 2 has no retained events")));
                }
                let trace = Trace::decode(std::fs::read(output)?.as_slice())?;
                let survivor = trace
                    .packets
                    .iter()
                    .filter_map(|packet| packet.ftrace.as_ref())
                    .find(|bundle| {
                        bundle
                            .events
                            .first()
                            .is_some_and(|event| event.timestamp == loss_base + 510)
                    })
                    .unwrap();
                // The loss belongs to this survivor even though skip_bytes > 0
                // makes the continuation's own decoded loss flag false.
                assert_eq!(survivor.lost, !cleanup);
            }
        }
        Ok(())
    }

    #[test]
    fn leading_history_hints_preserve_events_and_prevent_false_runnable_latency() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source.capture");
        let input = directory.path().join("unequal.capture");
        let output = directory.path().join("unequal.pftrace");
        std::fs::write(&source, fixture_capture(false, true)?)?;
        let mut reader = CaptureReader::open(&source)?;
        let mut capture = Vec::new();
        write_file_header(&mut capture)?;
        let mut sequence = 1;
        while let Some(record) = reader.next_record()? {
            if record.kind == RecordKind::End {
                let mut page = PAGE64.to_vec();
                // A fully consumed mmap prefix must neither establish coverage
                // nor consume the first-bundle hint before retained events arrive.
                page[..8].copy_from_slice(&199_000_000u64.to_le_bytes());
                let mut prefix = RecordHeader::new(RecordKind::Trace, sequence, 199_000_000);
                prefix.cpu = 1;
                prefix.committed_bytes = 276;
                prefix.skip_bytes = 276;
                write_record(&mut capture, prefix, &page)?;
                sequence += 1;
                page[..8].copy_from_slice(&200_000_000u64.to_le_bytes());
                let mut header = RecordHeader::new(RecordKind::Trace, sequence, 200_000_000);
                header.cpu = 1;
                header.committed_bytes = 276;
                write_record(&mut capture, header, &page)?;
                sequence += 1;
            }
            let mut header = RecordHeader::new(record.kind, sequence, record.timestamp_ns);
            header.cpu = record.cpu;
            header.committed_bytes = record.committed_bytes;
            header.skip_bytes = record.skip_bytes;
            write_record(&mut capture, header, &record.payload)?;
            sequence += 1;
        }
        std::fs::write(&input, capture)?;
        let summary = export(&input, &output)?;
        assert_eq!(summary.events, 8);
        let mut trace = Trace::decode(std::fs::read(&output)?.as_slice())?;
        let bundles = trace
            .packets
            .iter()
            .filter_map(|p| p.ftrace.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(bundles[0].previous_bundle_end_timestamp, Some(1510));
        assert_eq!(bundles[1].previous_bundle_end_timestamp, Some(200_000_510));
        assert!(bundles[..2].iter().all(|bundle| bundle.events.is_empty()));
        assert_eq!(
            bundles
                .iter()
                .map(|bundle| bundle.events.len())
                .sum::<usize>(),
            8
        );
        if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
            let query = "SELECT count(*) AS false_runnable FROM thread_state WHERE state = 'R' AND ts = 134219248";
            let run = |path: &Path, query: &str| -> Result<String> {
                let result = std::process::Command::new(&processor)
                    .arg(path)
                    .arg("-Q")
                    .arg(query)
                    .output()?;
                ensure!(
                    result.status.success(),
                    "Perfetto failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                Ok(String::from_utf8(result.stdout)?.trim().to_owned())
            };
            assert_eq!(run(&output, query)?, "\"false_runnable\"\n0");
            let coverage_query = "SELECT (SELECT int_value FROM metadata WHERE name = 'ftrace_latest_data_start_ns') AS boundary, EXISTS(SELECT 1 FROM sched WHERE ts >= 200000510) AS sched_survives, EXISTS(SELECT 1 FROM thread_state WHERE ts >= 200000510) AS states_survive, (SELECT count(*) FROM ftrace_event WHERE name = 'sched_waking' AND ts = 134219248) AS early_waking";
            assert_eq!(run(&output, coverage_query)?, "\"boundary\",\"sched_survives\",\"states_survive\",\"early_waking\"\n200000510,1,1,1");
            // Remove only the hints to demonstrate the otherwise identical
            // surviving events produce the misleading initial runnable interval.
            trace.packets.retain(|p| {
                !p.ftrace
                    .as_ref()
                    .is_some_and(|bundle| bundle.previous_bundle_end_timestamp.is_some())
            });
            let unhinted = directory.path().join("unhinted.pftrace");
            std::fs::write(&unhinted, trace.encode_to_vec())?;
            assert_eq!(run(&unhinted, query)?, "\"false_runnable\"\n1");
        }
        Ok(())
    }

    fn fixture_capture(big32: bool, complete: bool) -> Result<Vec<u8>> {
        let (manifest, files) = metadata(big32);
        let manifest_json = serde_json::json!({"schema_version":1,"endianness":manifest.endianness,"word_size":manifest.word_size,"subbuffer_size":4096,"trace_clock":"mono","cpus":[0,1],"header_page_ref":"events/header_page","header_event_ref":"events/header_event","clock_monotonic_ns":"100","clock_boottime_ns":"600","initial_tasks":[{"tid":42,"tgid":42,"comm":"worker"},{"tid":43,"tgid":42,"comm":"worker-child"}],"events":[{"id":100,"system":"sched","name":"sched_switch","format_ref":"sched_switch"},{"id":101,"system":"sched","name":"sched_waking","format_ref":"sched_waking"}]});
        let mut capture = Vec::new();
        write_file_header(&mut capture)?;
        let mut sequence = 1;
        let mut append = |kind, timestamp, cpu, committed, skip, payload: &[u8]| -> Result<()> {
            let mut header = RecordHeader::new(kind, sequence, timestamp);
            header.cpu = cpu;
            header.committed_bytes = committed;
            header.skip_bytes = skip;
            write_record(&mut capture, header, payload)?;
            sequence += 1;
            Ok(())
        };
        append(
            RecordKind::Metadata,
            0,
            u32::MAX,
            0,
            0,
            &serde_json::to_vec(&manifest_json)?,
        )?;
        for (path, bytes) in files {
            append(
                RecordKind::MetadataFile,
                0,
                u32::MAX,
                0,
                0,
                &metadata_file_payload(&path, &bytes)?,
            )?;
        }
        append(
            RecordKind::Trace,
            1000,
            0,
            if big32 { 264 } else { 276 },
            0,
            if big32 { PAGE32 } else { PAGE64 },
        )?;
        append(RecordKind::Trigger,2000,u32::MAX,0,0,br#"{"schema_version":1,"sequence":"7","timestamp_ns":"2000","end_timestamp_ns":"2000","description":"fixture fault"}"#)?;
        if complete {
            append(
                RecordKind::End,
                2000,
                u32::MAX,
                0,
                0,
                br#"{"schema_version":1,"completed":true}"#,
            )?;
        }
        Ok(capture)
    }

    #[test]
    fn native_perfetto_export_preserves_scheduling_clock_loss_and_trigger() -> Result<()> {
        for big32 in [false, true] {
            let temporary = tempfile::tempdir()?;
            let input = temporary.path().join("capture.retroft");
            let output = temporary.path().join("trace.pftrace");
            std::fs::write(&input, fixture_capture(big32, true)?)?;
            let summary = export(&input, &output)?;
            assert_eq!(summary.events, 4);
            assert_eq!(summary.triggers, 1);
            assert!(summary.completed);
            assert_eq!(summary.cpus[&0].lost_events, Some(9));
            assert_eq!(summary.cpus[&0].first_timestamp_ns, Some(1510));
            let trace = Trace::decode(std::fs::read(&output)?.as_slice())?;
            let bundle = trace
                .packets
                .iter()
                .filter_map(|p| p.ftrace.as_ref())
                .find(|bundle| bundle.previous_bundle_end_timestamp.is_none())
                .unwrap();
            assert_eq!(bundle.cpu, 0);
            assert!(bundle.lost);
            assert_eq!(bundle.events.len(), 4);
            assert!(bundle
                .events
                .iter()
                .all(|event| event.common_flags.is_none()));
            if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
                let result = std::process::Command::new(processor)
                    .arg(&output)
                    .arg("-Q")
                    .arg(
                        "SELECT count(*) AS events, sum(name IS NULL) AS unnamed FROM ftrace_event",
                    )
                    .output()?;
                ensure!(
                    result.status.success(),
                    "Perfetto failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                assert_eq!(
                    String::from_utf8(result.stdout)?.trim(),
                    "\"events\",\"unnamed\"\n4,0"
                );
            }
            let switch = bundle.events[0].switch.as_ref().unwrap();
            assert_eq!(switch.prev_pid, 0);
            assert_eq!(switch.next_pid, 42);
            assert_eq!(switch.next_comm, "worker");
            let waking = bundle.events[1].waking.as_ref().unwrap();
            assert_eq!(waking.pid, 43);
            assert_eq!(waking.target_cpu, 1);
            assert_eq!(bundle.events[2].switch.as_ref().unwrap().prev_state, -1);
            let trigger = trace.packets.iter().find(|p| p.track.is_some()).unwrap();
            assert_eq!(trigger.timestamp, Some(2500));
            assert_eq!(trigger.track.as_ref().unwrap().kind, 3);
            assert_eq!(
                trigger.track.as_ref().unwrap().name,
                "Trigger 7: fixture fault"
            );
            if let Some(directory) = std::env::var_os("RETROFT_TEST_ARTIFACT_DIR") {
                let directory = std::path::PathBuf::from(directory);
                std::fs::create_dir_all(&directory)?;
                let stem = if big32 { "big32" } else { "little64" };
                std::fs::copy(&input, directory.join(format!("{stem}.retroft")))?;
                std::fs::copy(&output, directory.join(format!("{stem}.pftrace")))?;
            }
        }
        Ok(())
    }

    fn snapshot_payload(mono: u64, boot: u64, reason: &str) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "monotonic_ns": mono.to_string(),
            "boottime_ns": boot.to_string(),
            "realtime_ns": (1_700_000_000_000_000_000u64 + boot).to_string(),
            "uncertainty_ns": "10", "reason": reason
        }))?)
    }

    #[test]
    fn validates_clock_snapshot_fields_and_optional_legacy_correlation() -> Result<()> {
        let payload = snapshot_payload(1500, 2500, "before_resume")?;
        validate_clock_snapshot(&payload, 1500)?;
        assert!(validate_clock_snapshot(&payload, 1501).is_err());
        for (field, invalid) in [
            ("schema_version", serde_json::json!(2)),
            ("reason", serde_json::json!("unknown")),
            ("monotonic_ns", serde_json::json!("01500")),
            ("boottime_ns", serde_json::json!(2500)),
            ("realtime_ns", serde_json::json!("18446744073709551616")),
            ("uncertainty_ns", serde_json::Value::Null),
        ] {
            let mut value: serde_json::Value = serde_json::from_slice(&payload)?;
            value[field] = invalid;
            assert!(
                validate_clock_snapshot(&serde_json::to_vec(&value)?, 1500).is_err(),
                "accepted invalid {field}"
            );
        }
        let (mut manifest, files) = metadata(false);
        manifest.clock_monotonic_ns = None;
        manifest.clock_boottime_ns = None;
        assert!(Decoder::new(&manifest, &files).is_err());
        let decoder =
            Decoder::with_clocks(&manifest, &files, &[ClockSample::parse(&payload, 1500)?])?;
        assert_eq!(decoder.page(PAGE64, 0, 276)?.events[0].timestamp, 1010);
        Ok(())
    }

    #[test]
    fn snapshot_prescan_deduplicates_and_ignores_records_after_end() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let input = temporary.path().join("capture");
        let sample = snapshot_payload(100, 200, "file_start")?;
        let mut capture = Vec::new();
        write_file_header(&mut capture)?;
        for sequence in 1..=2 {
            write_record(
                &mut capture,
                RecordHeader::new(RecordKind::ClockSnapshot, sequence, 100),
                &sample,
            )?;
        }
        let mut prefix = capture.clone();
        write_record(
            &mut capture,
            RecordHeader::new(RecordKind::End, 3, 100),
            br#"{"schema_version":1,"completed":true}"#,
        )?;
        write_record(
            &mut capture,
            RecordHeader::new(RecordKind::ClockSnapshot, 4, 100),
            b"invalid appended clock sample",
        )?;
        std::fs::write(&input, capture)?;
        assert_eq!(clock_samples(&input)?.len(), 1);
        write_record(
            &mut prefix,
            RecordHeader::new(RecordKind::ClockSnapshot, 3, 100),
            &snapshot_payload(100, 201, "resume")?,
        )?;
        std::fs::write(&input, prefix)?;
        assert!(clock_samples(&input).is_err());
        Ok(())
    }

    #[test]
    fn snapshot_export_spans_suspend_with_correlations_before_events() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let input = temporary.path().join("suspend.retroft");
        let output = temporary.path().join("suspend.pftrace");
        std::fs::write(&source, fixture_capture(false, true)?)?;
        let mut capture = Vec::new();
        write_file_header(&mut capture)?;
        let mut sequence = 1;
        let mut append = |kind, timestamp, cpu, committed, skip, payload: &[u8]| -> Result<()> {
            let mut header = RecordHeader::new(kind, sequence, timestamp);
            header.cpu = cpu;
            header.committed_bytes = committed;
            header.skip_bytes = skip;
            write_record(&mut capture, header, payload)?;
            sequence += 1;
            Ok(())
        };
        let mut reader = CaptureReader::open(&source)?;
        while let Some(record) = reader.next_record()? {
            if record.kind == RecordKind::Trace {
                // A file start precedes older retained history on disk. Both
                // sides of the transition are inside this single raw page.
                // The latest offset is 1ns smaller from sample jitter. A
                // BOOTTIME roundtrip would shift the preceding switch by 1ns.
                for (mono, boot, reason) in [
                    (268_437_468, 2_268_437_467, "file_start"),
                    (1500, 2500, "before_resume"),
                    (200_000_000, 2_200_000_000, "resume"),
                ] {
                    append(
                        RecordKind::ClockSnapshot,
                        mono,
                        u32::MAX,
                        0,
                        0,
                        &snapshot_payload(mono, boot, reason)?,
                    )?;
                }
            }
            if record.kind == RecordKind::End {
                for (id, timestamp) in [(8, 199_999_999), (9, 200_000_000)] {
                    let payload = serde_json::to_vec(
                        &serde_json::json!({"schema_version":1,"sequence":id.to_string(),"timestamp_ns":timestamp.to_string(),"end_timestamp_ns":timestamp.to_string(),"description":"boundary"}),
                    )?;
                    append(RecordKind::Trigger, timestamp, u32::MAX, 0, 0, &payload)?;
                }
                append(
                    RecordKind::Stats,
                    200_000_001,
                    u32::MAX,
                    0,
                    0,
                    br#"{"schema_version":1}"#,
                )?;
            }
            append(
                record.kind,
                record.timestamp_ns,
                record.cpu,
                record.committed_bytes,
                record.skip_bytes,
                &record.payload,
            )?;
        }
        std::fs::write(&input, capture)?;
        assert!(crate::capture::validate(&input)?.completed);
        let summary = export(&input, &output)?;
        assert!(summary.completed);
        assert_eq!(summary.clock_snapshots, 3);
        assert_eq!(summary.maximum_clock_sample_uncertainty_ns, Some(10));
        let trace = Trace::decode(std::fs::read(&output)?.as_slice())?;
        assert!(trace
            .packets
            .iter()
            .all(|packet| packet.sequence == Some(1)));
        let snapshots: Vec<_> = trace
            .packets
            .iter()
            .filter_map(|packet| packet.clock_snapshot.as_ref())
            .collect();
        assert_eq!(snapshots.len(), 3);
        for snapshot in snapshots {
            assert_eq!(snapshot.primary_trace_clock, 3);
            assert_eq!(
                snapshot
                    .clocks
                    .iter()
                    .map(|clock| clock.id)
                    .collect::<Vec<_>>(),
                [1, 3, 6, 64]
            );
            assert_eq!(snapshot.clocks[1].timestamp, snapshot.clocks[3].timestamp);
        }
        assert_eq!(
            trace
                .packets
                .iter()
                .find_map(|packet| packet.clock_snapshot.as_ref())
                .unwrap()
                .clocks[1]
                .timestamp,
            1500
        );
        let bundle = trace
            .packets
            .iter()
            .filter_map(|packet| packet.ftrace.as_ref())
            .find(|bundle| bundle.previous_bundle_end_timestamp.is_none())
            .unwrap();
        assert_eq!(bundle.clock, Some(2));
        assert_eq!(
            bundle
                .events
                .iter()
                .map(|event| event.timestamp)
                .collect::<Vec<_>>(),
            [1010, 134218748, 268437467, 268437474]
        );
        let markers: Vec<_> = trace
            .packets
            .iter()
            .filter(|packet| packet.track.is_some())
            .collect();
        assert_eq!(
            markers
                .iter()
                .map(|packet| packet.timestamp.unwrap())
                .collect::<Vec<_>>(),
            [2000, 199999999, 200000000, 200000001]
        );
        assert!(markers
            .iter()
            .all(|packet| packet.timestamp_clock_id == Some(3)));
        assert_eq!(summary.cpus[&0].first_timestamp_ns, Some(1010));
        assert_eq!(summary.cpus[&0].recovered_duration_ns, Some(268436464));
        if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
            let query = |sql: &str| -> Result<String> {
                let result = std::process::Command::new(&processor)
                    .arg("query")
                    .arg(&output)
                    .arg(sql)
                    .output()?;
                ensure!(
                    result.status.success(),
                    "Perfetto failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                Ok(String::from_utf8(result.stdout)?)
            };
            assert_eq!(
                query(
                    "SELECT s.ts,s.dur,t.tid FROM sched s JOIN thread t USING(utid) ORDER BY s.ts"
                )?
                .trim(),
                "\"ts\",\"dur\",\"tid\"\n1010,268436457,42\n268437467,7,43\n268437474,-1,0"
            );
            assert_eq!(query("SELECT ts,to_realtime(ts) AS utc FROM slice WHERE name LIKE 'Trigger%' ORDER BY ts")?.trim(), "\"ts\",\"utc\"\n2000,1700000000000003000\n199999999,1700000000200000999\n200000000,1700000002200000000");
            assert_eq!(
                query("SELECT COUNT(*) AS errors FROM stats WHERE severity='error' AND value!=0")?
                    .trim(),
                "\"errors\"\n0"
            );
        }
        if let Some(directory) = std::env::var_os("RETROFT_TEST_ARTIFACT_DIR") {
            std::fs::create_dir_all(&directory)?;
            std::fs::copy(input, Path::new(&directory).join("suspend.retroft"))?;
            std::fs::copy(output, Path::new(&directory).join("suspend.pftrace"))?;
        }
        Ok(())
    }

    #[test]
    fn reset_recovery_clears_mmap_continuity_and_marks_unknown_loss() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let input = temporary.path().join("capture");
        let output = temporary.path().join("trace");
        std::fs::write(&source, fixture_capture(false, true)?)?;
        let mut reader = CaptureReader::open(&source)?;
        let mut bytes = retroft_format::FILE_HEADER.to_vec();
        let mut sequence = 1;
        while let Some(record) = reader.next_record()? {
            let mut header = RecordHeader::new(record.kind, sequence, record.timestamp_ns);
            header.cpu = record.cpu;
            header.committed_bytes = record.committed_bytes;
            header.skip_bytes = record.skip_bytes;
            if record.kind == RecordKind::Trace {
                header.committed_bytes = 68;
                header.flags = retroft_format::HISTORY_RESET;
                write_record(&mut bytes, header, &record.payload)?;
                sequence += 1;
                header.sequence = sequence;
                header.committed_bytes = 116;
                header.skip_bytes = 68;
                let mut corrupt = Vec::new();
                write_record(&mut corrupt, header, &record.payload)?;
                *corrupt.last_mut().unwrap() ^= 1;
                bytes.extend(corrupt);
                sequence += 1;
                header.sequence = sequence;
                header.committed_bytes = 276;
                header.skip_bytes = 116;
                let packed = lz4::block::compress(&record.payload, None, false)?;
                let mut block = vec![0; retroft_format::HEADER_LEN + packed.len()];
                block[retroft_format::HEADER_LEN..].copy_from_slice(&packed);
                retroft_format::seal(
                    &mut block,
                    header,
                    packed.len(),
                    record.payload.len(),
                    retroft_format::CODEC_LZ4,
                )?;
                bytes.extend(block);
            } else {
                write_record(&mut bytes, header, &record.payload)?;
            }
            sequence += 1;
        }
        std::fs::write(&input, bytes)?;
        let summary = export(&input, &output)?;
        assert!(!summary.completed);
        assert!(summary.recovered_tail.is_some());
        assert_eq!(summary.events, 3);
        assert_eq!(summary.cpus[&0].lost_events, None);
        assert_eq!(summary.cpus[&0].lost_pages, 2);
        let trace = Trace::decode(std::fs::read(&output)?.as_slice())?;
        let bundles = trace
            .packets
            .iter()
            .filter_map(|p| p.ftrace.as_ref())
            .filter(|bundle| bundle.previous_bundle_end_timestamp.is_none())
            .collect::<Vec<_>>();
        assert_eq!(bundles.len(), 2);
        assert!(bundles[1].lost);
        assert_eq!(bundles[1].events[0].timestamp, 268437967);
        Ok(())
    }

    #[test]
    fn trigger_identity_is_exported_as_arguments_on_the_recorder_track() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let input = temporary.path().join("capture");
        let output = temporary.path().join("trace");
        std::fs::write(&source, fixture_capture(false, true)?)?;
        let mut reader = CaptureReader::open(&source)?;
        let mut bytes = retroft_format::FILE_HEADER.to_vec();
        while let Some(record) = reader.next_record()? {
            let mut payload = record.payload;
            if record.kind == RecordKind::Trigger {
                let mut value: serde_json::Value = serde_json::from_slice(&payload)?;
                for (key, field) in [
                    ("source", serde_json::json!("brake monitor")),
                    ("peer_process_name", serde_json::json!("controller")),
                    ("peer_pid", serde_json::json!(1234)),
                    ("peer_uid", serde_json::json!(1000)),
                    ("peer_gid", serde_json::json!(27)),
                ] {
                    value[key] = field;
                }
                payload = serde_json::to_vec(&value)?;
            }
            let mut header = RecordHeader::new(record.kind, record.sequence, record.timestamp_ns);
            header.cpu = record.cpu;
            header.committed_bytes = record.committed_bytes;
            header.skip_bytes = record.skip_bytes;
            write_record(&mut bytes, header, &payload)?;
        }
        std::fs::write(&input, bytes)?;
        export(&input, &output)?;
        let trace = Trace::decode(std::fs::read(&output)?.as_slice())?;
        let marker = trace.packets.iter().find_map(|p| p.track.as_ref()).unwrap();
        assert_eq!(marker.uuid, TRIGGER_TRACK);
        assert_eq!(marker.name, "Trigger 7 [brake monitor]: fixture fault");
        let args = marker
            .annotations
            .iter()
            .map(|a| (a.name.as_str(), a))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(args["source"].text.as_deref(), Some("brake monitor"));
        assert_eq!(args["description"].text.as_deref(), Some("fixture fault"));
        assert_eq!(
            args["peer_process_name"].text.as_deref(),
            Some("controller")
        );
        assert_eq!(args["peer_pid"].number, Some(1234));
        assert_eq!(args["peer_uid"].number, Some(1000));
        assert_eq!(args["peer_gid"].number, Some(27));
        if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
            let result = std::process::Command::new(processor).arg(&output).arg("-Q").arg(
                "SELECT a.key,a.display_value FROM slice s JOIN args a USING(arg_set_id) WHERE s.name LIKE 'Trigger%' ORDER BY a.key"
            ).output()?;
            ensure!(
                result.status.success(),
                "Perfetto failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert_eq!(
                String::from_utf8(result.stdout)?.trim(),
                concat!(
                    "\"key\",\"display_value\"\n",
                    "\"debug.description\",\"fixture fault\"\n",
                    "\"debug.peer_gid\",\"27\"\n",
                    "\"debug.peer_pid\",\"1234\"\n",
                    "\"debug.peer_process_name\",\"controller\"\n",
                    "\"debug.peer_uid\",\"1000\"\n",
                    "\"debug.source\",\"brake monitor\""
                )
            );
        }
        Ok(())
    }

    #[test]
    fn recovers_torn_tail_and_does_not_claim_completion() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let input = temporary.path().join("capture");
        let output = temporary.path().join("trace");
        let mut capture = fixture_capture(false, false)?;
        capture.extend_from_slice(b"FTRRpartial");
        std::fs::write(&input, capture)?;
        let summary = export(&input, &output)?;
        assert!(!summary.completed);
        assert_eq!(summary.events, 4);
        assert!(summary.recovered_tail.is_some());
        assert!(export(&input, &input).is_err());
        assert!(export(&input, &output).is_err());
        Ok(())
    }

    #[test]
    fn compressed_records_and_mmap_prefixes_export_the_same_scheduling() -> Result<()> {
        for (compressed, split) in [(true, false), (false, true), (true, true)] {
            let temporary = tempfile::tempdir()?;
            let source = temporary.path().join("source");
            let input = temporary.path().join("capture");
            let output = temporary.path().join("trace");
            std::fs::write(&source, fixture_capture(false, true)?)?;
            let mut reader = CaptureReader::open(&source)?;
            let mut bytes = Vec::new();
            write_file_header(&mut bytes)?;
            let mut sequence = 1;
            while let Some(record) = reader.next_record()? {
                let bounds = if split && record.kind == RecordKind::Trace {
                    vec![(68, 0), (276, 68)]
                } else {
                    vec![(record.committed_bytes, record.skip_bytes)]
                };
                for (committed, skip) in bounds {
                    let header = RecordHeader {
                        kind: record.kind,
                        cpu: record.cpu,
                        sequence,
                        timestamp_ns: record.timestamp_ns,
                        committed_bytes: committed,
                        skip_bytes: skip,
                        flags: if compressed && record.kind == RecordKind::Trace {
                            retroft_format::HISTORY_RESET
                        } else {
                            record.flags
                        },
                    };
                    sequence += 1;
                    if compressed && record.kind == RecordKind::Trace {
                        let stored = lz4::block::compress(&record.payload, None, false)?;
                        let mut block = vec![0; retroft_format::HEADER_LEN + stored.len()];
                        block[retroft_format::HEADER_LEN..].copy_from_slice(&stored);
                        retroft_format::seal(
                            &mut block,
                            header,
                            stored.len(),
                            record.payload.len(),
                            retroft_format::CODEC_LZ4,
                        )?;
                        bytes.extend_from_slice(&block);
                    } else {
                        write_record(&mut bytes, header, &record.payload)?;
                    }
                }
            }
            std::fs::write(&input, bytes)?;
            let summary = export(&input, &output)?;
            assert_eq!(summary.events, 4);
            assert_eq!(summary.cpus[&0].lost_pages, 1);
            assert!(summary.completed);
            let trace = Trace::decode(std::fs::read(&output)?.as_slice())?;
            let events = trace
                .packets
                .iter()
                .filter_map(|p| p.ftrace.as_ref())
                .flat_map(|b| b.events.iter())
                .collect::<Vec<_>>();
            assert_eq!(
                events
                    .iter()
                    .map(|event| event.timestamp)
                    .collect::<Vec<_>>(),
                [1510, 134219248, 268437967, 268437974]
            );
        }
        Ok(())
    }

    #[test]
    fn independent_captures_decode_a_reader_growing_across_the_file_boundary() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        std::fs::write(&source, fixture_capture(false, true)?)?;
        let mut reader = CaptureReader::open(&source)?;
        let mut records = Vec::new();
        while let Some(record) = reader.next_record()? {
            records.push(record);
        }

        let capture = |frontiers: &[(u32, u32)], chain: Option<(u64, &str)>| -> Result<Vec<u8>> {
            let mut bytes = Vec::new();
            write_file_header(&mut bytes)?;
            let mut sequence = 1;
            for record in &records {
                let ranges = if record.kind == RecordKind::Trace {
                    frontiers.to_vec()
                } else {
                    vec![(0, 0)]
                };
                for (committed, skip) in ranges {
                    let mut payload = record.payload.clone();
                    if let Some((index, session)) = chain {
                        if record.kind == RecordKind::Metadata {
                            let mut value: serde_json::Value = serde_json::from_slice(&payload)?;
                            value["session_id"] = session.into();
                            value["capture_chain"] = serde_json::json!({
                                "schema_version": 1, "capture_index": index.to_string(),
                                "previous_capture_index": if index == 1 { None } else { Some((index - 1).to_string()) }
                            });
                            payload = serde_json::to_vec(&value)?;
                        } else if record.kind == RecordKind::End {
                            let mut value: serde_json::Value = serde_json::from_slice(&payload)?;
                            value["capture_index"] = index.to_string().into();
                            payload = serde_json::to_vec(&value)?;
                        }
                    }
                    if record.kind == RecordKind::Trace && committed == 68 {
                        // The first acquisition sees only the initial switch.
                        // At the next capture this same raw reader has grown.
                        payload[8..16].copy_from_slice(&(68u64 | (1 << 31)).to_le_bytes());
                        payload[16 + 68..].fill(0);
                    }
                    write_record(
                        &mut bytes,
                        RecordHeader {
                            kind: record.kind,
                            cpu: record.cpu,
                            sequence,
                            timestamp_ns: record.timestamp_ns,
                            committed_bytes: committed,
                            skip_bytes: skip,
                            flags: record.flags,
                        },
                        &payload,
                    )?;
                    sequence += 1;
                }
            }
            Ok(bytes)
        };

        let mut combined_timestamps = Vec::new();
        for (name, ranges, expected_events, expected_loss_pages) in [
            ("first", vec![(68, 0)], 1, 1),
            ("second", vec![(276, 68)], 3, 0),
        ] {
            let input = temporary.path().join(format!("{name}.retroft"));
            let output = temporary.path().join(format!("{name}.pftrace"));
            std::fs::write(&input, capture(&ranges, None)?)?;
            // Each call constructs its own reader and decoder; no shared state.
            let summary = export(&input, &output)?;
            assert!(summary.completed);
            assert_eq!(summary.events, expected_events);
            assert_eq!(summary.cpus[&0].lost_pages, expected_loss_pages);
            let trace = Trace::decode(std::fs::read(output)?.as_slice())?;
            combined_timestamps.extend(
                trace
                    .packets
                    .iter()
                    .filter_map(|p| p.ftrace.as_ref())
                    .flat_map(|bundle| bundle.events.iter().map(|event| event.timestamp)),
            );
        }
        assert_eq!(combined_timestamps, [1510, 134219248, 268437967, 268437974]);

        // Joining must preserve the same events and scheduler state as one
        // continuous export, including the reader prefix spanning both files.
        let first = temporary.path().join("chain-1.retroft");
        let second = temporary.path().join("chain-2.retroft");
        let merged = temporary.path().join("merged.pftrace");
        let continuous = temporary.path().join("continuous.pftrace");
        std::fs::write(&first, capture(&[(68, 0)], Some((1, "fixture-session")))?)?;
        std::fs::write(
            &second,
            capture(&[(276, 68)], Some((2, "fixture-session")))?,
        )?;
        let summary = export_captures(&[&first, &second], &merged)?;
        assert!(summary.completed);
        assert_eq!(summary.events, 4);
        assert_eq!(summary.cpus[&0].lost_pages, 1);
        export(&source, &continuous)?;
        let merged_trace = Trace::decode(std::fs::read(&merged)?.as_slice())?;
        let continuous_trace = Trace::decode(std::fs::read(&continuous)?.as_slice())?;
        let event_bytes = |trace: &Trace| {
            trace
                .packets
                .iter()
                .filter_map(|packet| packet.ftrace.as_ref())
                .flat_map(|bundle| bundle.events.iter().map(Message::encode_to_vec))
                .collect::<Vec<_>>()
        };
        assert_eq!(event_bytes(&merged_trace), event_bytes(&continuous_trace));
        // No extra initialization or synthetic loss at the file boundary.
        assert_eq!(
            merged_trace
                .packets
                .iter()
                .filter(|packet| packet.ftrace.as_ref().is_some_and(|b| b.lost))
                .count(),
            1
        );
        if let Some(processor) = std::env::var_os("RETROFT_TEST_PERFETTO") {
            for query in [
                "SELECT ts,dur,cpu,utid,end_state FROM sched ORDER BY ts,cpu",
                "SELECT ts,dur,utid,state FROM thread_state ORDER BY ts,utid,state",
            ] {
                let query_trace = |path: &Path| -> Result<Vec<u8>> {
                    let result = std::process::Command::new(&processor)
                        .arg(path)
                        .arg("-Q")
                        .arg(query)
                        .output()?;
                    ensure!(
                        result.status.success(),
                        "Perfetto failed: {}",
                        String::from_utf8_lossy(&result.stderr)
                    );
                    Ok(result.stdout)
                };
                assert_eq!(query_trace(&merged)?, query_trace(&continuous)?);
            }
        }
        // Structural gaps must fail before creating output.
        for (name, inputs) in [
            ("reordered", vec![second.as_path(), first.as_path()]),
            ("duplicate", vec![first.as_path(), first.as_path()]),
            ("legacy", vec![source.as_path(), second.as_path()]),
        ] {
            let output = temporary.path().join(name);
            assert!(export_captures(&inputs, &output).is_err());
            assert!(!output.exists());
        }
        let rejected = temporary.path().join("rejected.pftrace");
        std::fs::write(
            &second,
            capture(&[(276, 68)], Some((3, "fixture-session")))?,
        )?;
        assert!(export_captures(&[&first, &second], &rejected).is_err());
        assert!(!rejected.exists());
        std::fs::write(&second, capture(&[(276, 68)], Some((2, "other-session")))?)?;
        assert!(export_captures(&[&first, &second], &rejected).is_err());
        assert!(!rejected.exists());
        let mut torn = capture(&[(276, 68)], Some((2, "fixture-session")))?;
        torn.truncate(torn.len() - 1);
        std::fs::write(&second, torn)?;
        assert!(export_captures(&[&first, &second], &rejected).is_err());
        assert!(!rejected.exists());
        std::fs::write(
            &second,
            capture(&[(276, 76)], Some((2, "fixture-session")))?,
        )?;
        assert!(export_captures(&[&first, &second], &rejected)
            .unwrap_err()
            .to_string()
            .contains("mmap continuation"));

        // A later page within one file must still resume at its previous frontier,
        // even when another boundary (76) would be independently parsable.
        let invalid = temporary.path().join("invalid.retroft");
        std::fs::write(&invalid, capture(&[(68, 0), (276, 76)], None)?)?;
        let error = export(&invalid, &temporary.path().join("invalid.pftrace")).unwrap_err();
        assert!(error
            .to_string()
            .contains("mmap continuation does not follow previous CPU prefix"));
        Ok(())
    }
}
