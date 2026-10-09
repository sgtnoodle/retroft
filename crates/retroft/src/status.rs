//! Owned status events for recorder integrations. No bus or logging dependency.
use serde::Serialize;
use std::path::PathBuf;

/// Trigger accepted by the recorder. A successful client send alone does not
/// establish acceptance.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct TriggerStatus {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub source: Option<String>,
    pub pid: Option<i32>,
    pub description: String,
    pub post_remaining_ns: u64,
}

/// Live-buffer statistics for one CPU, sampled without consuming events.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct CpuIdleStatus {
    pub cpu: u32,
    pub entries: Option<u64>,
    /// None for an empty buffer or unavailable timestamps; consult entries.
    pub oldest_age_ns: Option<u64>,
}

/// Events are delivered synchronously on the recorder thread. Hand off expensive
/// work with a nonblocking queue; a slow or panicking callback disrupts recording.
/// This enum is extensible: consumers must allow unknown variants in Rust and
/// should version their own bus schema rather than treating its JSON as a protocol.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum StatusEvent {
    Ready {
        session_id: String,
        output: PathBuf,
        cpu_count: usize,
        summary: String,
    },
    CaptureStarted {
        path: PathBuf,
        capture_index: u64,
        trigger: TriggerStatus,
    },
    TriggerAdded {
        path: PathBuf,
        capture_index: u64,
        trigger: TriggerStatus,
        extension_ns: u64,
    },
    /// Emitted only after final file sync and creation-time directory sync succeed.
    CaptureCompleted {
        path: PathBuf,
        capture_index: u64,
        bytes: u64,
        elapsed_ns: u64,
        triggers: u64,
        snapshot_banks: u64,
    },
    Overrun {
        cpu: u32,
        path: PathBuf,
    },
    Idle {
        cpus: Vec<CpuIdleStatus>,
    },
    /// Diagnostic text for people. Integrations should not parse it.
    Diagnostic {
        message: String,
    },
}
