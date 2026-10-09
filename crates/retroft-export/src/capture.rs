//! Offline capture reading, recovery, decompression, and validation.
use anyhow::{ensure, Context, Result};
use retroft_format::{
    RecordKind, FILE_HEADER, FILE_HEADER_LEN, HEADER_LEN, HISTORY_RESET, MAX_PAYLOAD,
};
use serde::Serialize;
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

const HISTORY_BYTES: usize = 64 * 1024;

extern "C" {
    // Stable public LZ4 API omitted from the Rust crate's bindings.
    fn LZ4_decompress_safe_usingDict(
        source: *const u8,
        dest: *mut u8,
        source_size: i32,
        dest_capacity: i32,
        dictionary: *const u8,
        dictionary_size: i32,
    ) -> i32;
}

#[derive(Debug, Clone)]
pub struct Record {
    pub kind: RecordKind,
    pub cpu: u32,
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub committed_bytes: u32,
    pub skip_bytes: u32,
    pub flags: u32,
    /// This reset record follows skipped corrupt records; continuity was lost.
    pub recovered_after_gap: bool,
    pub payload: Vec<u8>,
}

pub struct CaptureReader {
    file: BufReader<File>,
    pub recovered_tail: Option<String>,
    pub records_read: u64,
    last_sequence: u64,
    done: bool,
    history: Vec<u8>,
    history_reset_seen: bool,
    stream_started: bool,
    end_seen: bool,
    recovery_gaps: u64,
}
impl CaptureReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut header = [0; FILE_HEADER_LEN];
        file.read_exact(&mut header)
            .context("incomplete file header")?;
        ensure!(
            header == FILE_HEADER,
            "unsupported capture magic or version"
        );
        Ok(Self {
            file: BufReader::new(file),
            recovered_tail: None,
            records_read: 0,
            last_sequence: 0,
            done: false,
            history: Vec::with_capacity(HISTORY_BYTES),
            history_reset_seen: false,
            stream_started: false,
            end_seen: false,
            recovery_gaps: 0,
        })
    }
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        if self.done {
            return Ok(None);
        }
        let start = self.file.stream_position()?;
        match self.read_record(false) {
            Ok(Some(r)) => {
                self.accept(&r);
                Ok(Some(r))
            }
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Err(e) => {
                let reason = format!("after {} valid records: {e:#}", self.records_read);
                // Missing startup metadata cannot be reconstructed from a reset.
                let recovered = if self.stream_started && !self.end_seen {
                    self.recover_record(start + 1)
                } else {
                    Ok(None)
                };
                match recovered {
                    Ok(Some(mut record)) => {
                        self.recovery_gaps += 1;
                        record.recovered_after_gap = true;
                        self.recovered_tail = Some(format!(
                            "{reason}; recovered at sequence {}; recovery gaps={}",
                            record.sequence, self.recovery_gaps
                        ));
                        self.accept(&record);
                        Ok(Some(record))
                    }
                    result => {
                        self.done = true;
                        let detail = result
                            .err()
                            .map(|error| format!("; reset scan failed: {error:#}"))
                            .unwrap_or_default();
                        self.recovered_tail = Some(format!(
                            "{reason}; recovery gaps={}{}",
                            self.recovery_gaps, detail
                        ));
                        Ok(None)
                    }
                }
            }
        }
    }
    fn accept(&mut self, record: &Record) {
        self.records_read += 1;
        self.last_sequence = record.sequence;
        self.stream_started |=
            !matches!(record.kind, RecordKind::Metadata | RecordKind::MetadataFile);
        self.end_seen |= record.kind == RecordKind::End;
    }

    fn recover_record(&mut self, start: u64) -> Result<Option<Record>> {
        self.history.clear();
        self.history_reset_seen = false;
        self.file.seek(SeekFrom::Start(start))?;
        let mut magic = [0u8; 4];
        loop {
            if let Err(error) = self.file.read_exact(&mut magic) {
                return if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    Ok(None)
                } else {
                    Err(error.into())
                };
            }
            loop {
                if &magic == b"FTRR" {
                    let candidate = self.file.stream_position()? - 4;
                    self.file.seek(SeekFrom::Start(candidate))?;
                    if let Ok(Some(record)) = self.read_record(true) {
                        return Ok(Some(record));
                    }
                    // Ignore false magic/CRC candidates without trusting lengths.
                    self.file.seek(SeekFrom::Start(candidate + 1))?;
                    break;
                }
                magic.copy_within(1.., 0);
                if let Err(error) = self.file.read_exact(&mut magic[3..]) {
                    return if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        Ok(None)
                    } else {
                        Err(error.into())
                    };
                }
            }
        }
    }

    fn read_record(&mut self, recovering: bool) -> Result<Option<Record>> {
        let mut h = [0u8; HEADER_LEN];
        let n = loop {
            match self.file.read(&mut h[..1]) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                r => break r?,
            }
        };
        if n == 0 {
            return Ok(None);
        }
        self.file
            .read_exact(&mut h[1..])
            .context("incomplete record header")?;
        ensure!(
            &h[..4] == b"FTRR" && u16::from_le_bytes(h[4..6].try_into().unwrap()) == 1,
            "invalid record header"
        );
        let u32_at = |i| u32::from_le_bytes(h[i..i + 4].try_into().unwrap());
        let u64_at = |i| u64::from_le_bytes(h[i..i + 8].try_into().unwrap());
        let kind = RecordKind::try_from(u16::from_le_bytes(h[6..8].try_into().unwrap()))?;
        let codec = u32_at(8);
        let cpu = u32_at(12);
        let sequence = u64_at(16);
        let timestamp_ns = u64_at(24);
        let stored_len = u32_at(32) as usize;
        let raw_len = u32_at(36) as usize;
        let committed_bytes = u32_at(40);
        let skip_bytes = u32_at(44);
        let flags = u32_at(48);
        let expected = u32_at(56);
        ensure!(
            flags & !HISTORY_RESET == 0 && h[52..56] == [0; 4] && h[60..64] == [0; 4],
            "unsupported record flags"
        );
        ensure!(
            if recovering {
                sequence > self.last_sequence
            } else {
                sequence
                    == self
                        .last_sequence
                        .checked_add(1)
                        .context("sequence overflow")?
            },
            "noncontiguous record sequence"
        );
        ensure!(
            matches!(codec, 0 | 2) && stored_len <= MAX_PAYLOAD && raw_len <= MAX_PAYLOAD,
            "invalid codec or payload length"
        );
        ensure!(
            skip_bytes <= committed_bytes && committed_bytes as usize <= raw_len,
            "invalid raw page bounds"
        );
        if kind != RecordKind::Trace {
            ensure!(
                skip_bytes == 0 && committed_bytes == 0 && flags == 0 && codec == 0,
                "unexpected non-trace page bounds/codec/flags"
            );
        }
        if recovering {
            ensure!(
                kind == RecordKind::Trace && flags & HISTORY_RESET != 0,
                "recovery requires a reset trace record"
            );
        }
        let mut payload = vec![0; stored_len];
        self.file
            .read_exact(&mut payload)
            .context("incomplete record payload")?;
        h[56..60].fill(0);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&h), &payload);
        ensure!(crc == expected, "record checksum mismatch");
        if flags & HISTORY_RESET != 0 {
            self.history.clear();
            self.history_reset_seen = true;
        }
        if codec == 0 {
            ensure!(stored_len == raw_len, "raw length mismatch");
        } else {
            ensure!(
                self.history_reset_seen,
                "linked LZ4 requires an initial history reset"
            );
            payload = decode_lz4(&payload, raw_len, &self.history)?;
        }
        if kind == RecordKind::Trace {
            remember(&mut self.history, &payload);
        }
        Ok(Some(Record {
            kind,
            cpu,
            sequence,
            timestamp_ns,
            committed_bytes,
            skip_bytes,
            flags,
            recovered_after_gap: false,
            payload,
        }))
    }
}

fn remember(history: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.len() >= HISTORY_BYTES {
        history.clear();
        history.extend_from_slice(&bytes[bytes.len() - HISTORY_BYTES..]);
    } else {
        let discard = (history.len() + bytes.len()).saturating_sub(HISTORY_BYTES);
        history.copy_within(discard.., 0);
        history.truncate(history.len() - discard);
        history.extend_from_slice(bytes);
    }
}

fn decode_lz4(stored: &[u8], raw_len: usize, history: &[u8]) -> Result<Vec<u8>> {
    let source_size = i32::try_from(stored.len())?;
    let dest_capacity = i32::try_from(raw_len)?;
    let dictionary_size = i32::try_from(history.len())?;
    let mut output = vec![0; raw_len];
    // SAFETY: separate initialized input/dictionary/output allocations stay live
    // and unchanged during this stable, bounds-checking LZ4 call. All declared
    // sizes match their slices and raw_len was bounded by MAX_PAYLOAD.
    let decoded = unsafe {
        if history.is_empty() {
            lz4::liblz4::LZ4_decompress_safe(
                stored.as_ptr().cast(),
                output.as_mut_ptr().cast(),
                source_size,
                dest_capacity,
            )
        } else {
            LZ4_decompress_safe_usingDict(
                stored.as_ptr(),
                output.as_mut_ptr(),
                source_size,
                dest_capacity,
                history.as_ptr(),
                dictionary_size,
            )
        }
    };
    ensure!(
        decoded >= 0 && decoded as usize == raw_len,
        "invalid linked LZ4 block/decoded length"
    );
    Ok(output)
}

#[derive(Debug, Serialize)]
pub struct Validation {
    pub records: u64,
    pub trace_records: u64,
    pub completed: bool,
    pub recovered_tail: Option<String>,
}
pub fn validate(path: &Path) -> Result<Validation> {
    let mut reader = CaptureReader::open(path)?;
    let mut trace_records = 0;
    let mut completed = false;
    let mut manifest_seen = false;
    let mut stream_started = false;
    let mut end_seen = false;
    while let Some(r) = reader.next_record()? {
        ensure!(!end_seen, "record after capture end");
        if !manifest_seen {
            ensure!(
                r.kind == RecordKind::Metadata,
                "capture must begin with a manifest"
            );
        }
        match r.kind {
            RecordKind::Metadata => {
                ensure!(!manifest_seen, "duplicate capture manifest");
                manifest_seen = true;
            }
            RecordKind::MetadataFile => {
                ensure!(
                    !stream_started,
                    "metadata file after capture stream started"
                );
                ensure!(r.payload.len() >= 4, "incomplete metadata file path");
                let n = u32::from_le_bytes(r.payload[..4].try_into().unwrap()) as usize;
                ensure!(
                    n > 0 && n <= 4096 && n <= r.payload.len() - 4,
                    "invalid metadata file path length"
                );
                std::str::from_utf8(&r.payload[4..4 + n])
                    .context("invalid metadata file path encoding")?;
            }
            _ => stream_started = true,
        }
        if r.kind == RecordKind::ClockSnapshot {
            crate::offline::validate_clock_snapshot(&r.payload, r.timestamp_ns)?;
        }
        if matches!(
            r.kind,
            RecordKind::Metadata | RecordKind::Trigger | RecordKind::Stats | RecordKind::End
        ) {
            let json: serde_json::Value =
                serde_json::from_slice(&r.payload).context("invalid JSON metadata")?;
            ensure!(
                json.is_object() && json["schema_version"].as_u64() == Some(1),
                "unsupported metadata schema"
            );
            if r.kind == RecordKind::End {
                end_seen = true;
                completed = json["completed"]
                    .as_bool()
                    .context("end record missing completion status")?;
            }
        }
        if r.kind == RecordKind::Trace {
            trace_records += 1;
        }
    }
    Ok(Validation {
        records: reader.records_read,
        trace_records,
        completed: manifest_seen && completed && reader.recovered_tail.is_none(),
        recovered_tail: reader.recovered_tail,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use retroft_format::{seal, write_record, RecordHeader};
    #[test]
    fn crc_known_vector() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xe3069283);
    }
    #[test]
    fn completion_requires_manifest_and_terminal_end() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture");
        let mut bytes = FILE_HEADER.to_vec();
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::End, 1, 0),
            br#"{"schema_version":1,"completed":true}"#,
        )?;
        std::fs::write(&path, &bytes)?;
        assert!(validate(&path).is_err());
        bytes.truncate(FILE_HEADER_LEN);
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Metadata, 1, 0),
            br#"{"schema_version":1}"#,
        )?;
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::End, 2, 0),
            br#"{"schema_version":1,"completed":false}"#,
        )?;
        std::fs::write(&path, &bytes)?;
        assert!(!validate(&path)?.completed);
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Trigger, 3, 0),
            br#"{"schema_version":1}"#,
        )?;
        std::fs::write(&path, &bytes)?;
        assert!(validate(&path).is_err());
        Ok(())
    }
    #[test]
    fn preserves_prefix_after_torn_or_corrupt_tail() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture");
        let mut bytes = FILE_HEADER.to_vec();
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Trigger, 1, 4),
            b"one",
        )?;
        let boundary = bytes.len();
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Trigger, 2, 5),
            b"two",
        )?;
        for cut in boundary + 1..bytes.len() {
            std::fs::write(&path, &bytes[..cut])?;
            let mut r = CaptureReader::open(&path)?;
            assert_eq!(r.next_record()?.unwrap().payload, b"one");
            assert!(r.next_record()?.is_none());
            assert!(r.recovered_tail.is_some());
        }
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&path, &bytes)?;
        let mut r = CaptureReader::open(&path)?;
        assert!(r.next_record()?.is_some());
        assert!(r.next_record()?.is_none());
        assert!(r.recovered_tail.unwrap().contains("checksum"));
        Ok(())
    }
    fn trace_block(
        sequence: u64,
        cpu: u32,
        flags: u32,
        raw_len: usize,
        stored: &[u8],
        codec: u32,
    ) -> Result<Vec<u8>> {
        let mut header = RecordHeader::new(RecordKind::Trace, sequence, 0);
        header.cpu = cpu;
        header.flags = flags;
        let mut block = vec![0; HEADER_LEN + stored.len()];
        block[HEADER_LEN..].copy_from_slice(stored);
        seal(&mut block, header, stored.len(), raw_len, codec)?;
        Ok(block)
    }

    // Independent LZ4 fixture: match 16 bytes at offset 16 from prior history,
    // then five final literals. There are no literals supplying the match.
    const DICTIONARY_MATCH: &[u8] = &[0x0c, 16, 0, 0x50, b't', b'a', b'i', b'l', b'!'];

    #[test]
    fn linked_lz4_uses_raw_history_across_cpus_but_not_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture");
        let first = b"0123456789abcdef";
        let next = b"FEDCBA9876543210";
        let mut bytes = FILE_HEADER.to_vec();
        bytes.extend(trace_block(1, 0, HISTORY_RESET, first.len(), first, 0)?);
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Stats, 2, 0),
            b"excluded history",
        )?;
        bytes.extend(trace_block(3, 1, 0, 21, DICTIONARY_MATCH, 2)?);
        bytes.extend(trace_block(4, 2, 0, next.len(), next, 0)?);
        bytes.extend(trace_block(5, 0, 0, 21, DICTIONARY_MATCH, 2)?);
        std::fs::write(&path, bytes)?;
        let mut reader = CaptureReader::open(&path)?;
        assert_eq!(reader.next_record()?.unwrap().payload, first);
        assert_eq!(reader.next_record()?.unwrap().payload, b"excluded history");
        assert_eq!(
            reader.next_record()?.unwrap().payload,
            b"0123456789abcdeftail!"
        );
        assert_eq!(reader.next_record()?.unwrap().payload, next);
        assert_eq!(
            reader.next_record()?.unwrap().payload,
            b"FEDCBA9876543210tail!"
        );
        assert!(reader.next_record()?.is_none());
        assert!(reader.recovered_tail.is_none());
        assert!(decode_lz4(DICTIONARY_MATCH, 21, &[]).is_err());
        assert!(decode_lz4(DICTIONARY_MATCH, 22, first).is_err());
        Ok(())
    }

    #[test]
    fn history_retains_exact_last_64k_and_reset_is_required() -> Result<()> {
        let mut history = Vec::new();
        remember(&mut history, &vec![1; HISTORY_BYTES - 8]);
        remember(&mut history, &[2; 16]);
        assert_eq!(history.len(), HISTORY_BYTES);
        assert_eq!(&history[HISTORY_BYTES - 16..], &[2; 16]);
        remember(&mut history, &vec![3; HISTORY_BYTES + 1]);
        assert_eq!(history, vec![3; HISTORY_BYTES]);

        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture");
        for reset_second in [false, true] {
            let mut bytes = FILE_HEADER.to_vec();
            bytes.extend(trace_block(1, 0, 0, 16, b"0123456789abcdef", 0)?);
            bytes.extend(trace_block(
                2,
                1,
                if reset_second { HISTORY_RESET } else { 0 },
                21,
                DICTIONARY_MATCH,
                2,
            )?);
            std::fs::write(&path, bytes)?;
            let mut reader = CaptureReader::open(&path)?;
            assert!(reader.next_record()?.is_some());
            assert!(reader.next_record()?.is_none());
            assert!(reader.recovered_tail.is_some());
        }
        Ok(())
    }

    #[test]
    fn corrupt_stream_recovers_only_verified_reset_and_stays_incomplete() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture");
        let mut bytes = FILE_HEADER.to_vec();
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Metadata, 1, 0),
            br#"{"schema_version":1}"#,
        )?;
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Stats, 2, 0),
            br#"{"schema_version":1}"#,
        )?;
        bytes.extend(trace_block(
            3,
            0,
            HISTORY_RESET,
            16,
            b"0123456789abcdef",
            0,
        )?);
        let mut corrupt = trace_block(4, 0, HISTORY_RESET, 16, b"bad CRC reset!!!", 0)?;
        *corrupt.last_mut().unwrap() ^= 1;
        bytes.extend(corrupt);
        // CRC-valid non-reset cannot restart dictionary; stale sequences cannot either.
        bytes.extend(trace_block(5, 1, 0, 21, DICTIONARY_MATCH, 2)?);
        bytes.extend(trace_block(
            2,
            1,
            HISTORY_RESET,
            16,
            b"stale sequence!!",
            0,
        )?);
        bytes.extend(trace_block(
            6,
            1,
            HISTORY_RESET,
            16,
            b"FEDCBA9876543210",
            0,
        )?);
        bytes.extend(trace_block(7, 0, 0, 21, DICTIONARY_MATCH, 2)?);
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::End, 8, 0),
            br#"{"schema_version":1,"completed":true}"#,
        )?;
        std::fs::write(&path, bytes)?;
        let mut reader = CaptureReader::open(&path)?;
        for sequence in 1..=3 {
            assert_eq!(reader.next_record()?.unwrap().sequence, sequence);
        }
        let recovered = reader.next_record()?.unwrap();
        assert_eq!(recovered.sequence, 6);
        assert!(recovered.recovered_after_gap);
        assert_eq!(recovered.payload, b"FEDCBA9876543210");
        let linked = reader.next_record()?.unwrap();
        assert!(!linked.recovered_after_gap);
        assert_eq!(linked.payload, b"FEDCBA9876543210tail!");
        assert_eq!(reader.next_record()?.unwrap().kind, RecordKind::End);
        assert!(reader.next_record()?.is_none());
        let validation = validate(&path)?;
        assert_eq!(validation.trace_records, 3);
        assert!(!validation.completed);
        assert!(validation
            .recovered_tail
            .unwrap()
            .contains("recovered at sequence 6"));
        Ok(())
    }

    #[test]
    fn corrupt_startup_metadata_and_obsolete_codec_are_not_recovered() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture");
        let mut bytes = FILE_HEADER.to_vec();
        write_record(
            &mut bytes,
            RecordHeader::new(RecordKind::Metadata, 1, 0),
            br#"{"schema_version":1}"#,
        )?;
        let mut corrupt = Vec::new();
        write_record(
            &mut corrupt,
            RecordHeader::new(RecordKind::MetadataFile, 2, 0),
            b"bad metadata",
        )?;
        *corrupt.last_mut().unwrap() ^= 1;
        bytes.extend(corrupt);
        bytes.extend(trace_block(
            3,
            0,
            HISTORY_RESET,
            16,
            b"0123456789abcdef",
            0,
        )?);
        std::fs::write(&path, bytes)?;
        let mut reader = CaptureReader::open(&path)?;
        assert!(reader.next_record()?.is_some());
        assert!(reader.next_record()?.is_none());
        assert_eq!(reader.records_read, 1);
        let mut old = trace_block(1, 0, HISTORY_RESET, 16, b"0123456789abcdef", 0)?;
        old[8..12].copy_from_slice(&1u32.to_le_bytes());
        old[56..60].fill(0);
        let crc = crc32c::crc32c(&old);
        old[56..60].copy_from_slice(&crc.to_le_bytes());
        let mut bytes = FILE_HEADER.to_vec();
        bytes.extend(old);
        std::fs::write(&path, bytes)?;
        let mut reader = CaptureReader::open(&path)?;
        assert!(reader.next_record()?.is_none());
        assert!(reader.recovered_tail.unwrap().contains("codec"));
        Ok(())
    }
}
