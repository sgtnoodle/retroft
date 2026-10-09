//! Versioned append-only container. The cyclic redundancy check (CRC) covers
//! the encoded header and stored bytes.
use anyhow::{bail, ensure, Result};
use std::io::Write;

pub const FILE_HEADER_LEN: usize = 16;
pub const HEADER_LEN: usize = 64;
/// Starts a new compression history before this trace payload.
pub const HISTORY_RESET: u32 = 1;
pub const CODEC_RAW: u32 = 0;
pub const CODEC_LZ4: u32 = 2;
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
pub const FILE_HEADER: [u8; FILE_HEADER_LEN] = *b"FTRREC01\x01\0\0\0\0\0\0\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RecordKind {
    Metadata = 1,
    MetadataFile = 2,
    Trace = 3,
    Trigger = 4,
    Stats = 5,
    End = 6,
    ClockSnapshot = 7,
}
impl TryFrom<u16> for RecordKind {
    type Error = anyhow::Error;
    fn try_from(n: u16) -> Result<Self> {
        Ok(match n {
            1 => Self::Metadata,
            2 => Self::MetadataFile,
            3 => Self::Trace,
            4 => Self::Trigger,
            5 => Self::Stats,
            6 => Self::End,
            7 => Self::ClockSnapshot,
            _ => bail!("unsupported record kind {n}"),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub kind: RecordKind,
    pub cpu: u32,
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub committed_bytes: u32,
    pub skip_bytes: u32,
    pub flags: u32,
}
impl RecordHeader {
    pub fn new(kind: RecordKind, sequence: u64, timestamp_ns: u64) -> Self {
        Self {
            kind,
            cpu: u32::MAX,
            sequence,
            timestamp_ns,
            committed_bytes: 0,
            skip_bytes: 0,
            flags: 0,
        }
    }
}

pub fn write_file_header(file: &mut impl Write) -> Result<()> {
    file.write_all(&FILE_HEADER)?;
    Ok(())
}

/// Payload is already in the output block at HEADER_LEN. Does not allocate.
pub fn seal(
    block: &mut [u8],
    header: RecordHeader,
    stored_len: usize,
    raw_len: usize,
    codec: u32,
) -> Result<usize> {
    ensure!(
        stored_len <= MAX_PAYLOAD && raw_len <= MAX_PAYLOAD,
        "record exceeds size limit"
    );
    ensure!(
        block.len() >= HEADER_LEN + stored_len,
        "record output too small"
    );
    ensure!(matches!(codec, CODEC_RAW | CODEC_LZ4), "unknown codec");
    ensure!(header.flags & !HISTORY_RESET == 0, "unknown record flags");
    ensure!(
        header.kind == RecordKind::Trace || (codec == CODEC_RAW && header.flags == 0),
        "compression only supported for trace records"
    );
    let h = &mut block[..HEADER_LEN];
    h.fill(0);
    h[0..4].copy_from_slice(b"FTRR");
    h[4..6].copy_from_slice(&1u16.to_le_bytes());
    h[6..8].copy_from_slice(&(header.kind as u16).to_le_bytes());
    h[8..12].copy_from_slice(&codec.to_le_bytes());
    h[12..16].copy_from_slice(&header.cpu.to_le_bytes());
    h[16..24].copy_from_slice(&header.sequence.to_le_bytes());
    h[24..32].copy_from_slice(&header.timestamp_ns.to_le_bytes());
    h[32..36].copy_from_slice(&(stored_len as u32).to_le_bytes());
    h[36..40].copy_from_slice(&(raw_len as u32).to_le_bytes());
    h[40..44].copy_from_slice(&header.committed_bytes.to_le_bytes());
    h[44..48].copy_from_slice(&header.skip_bytes.to_le_bytes());
    h[48..52].copy_from_slice(&header.flags.to_le_bytes());
    let len = HEADER_LEN + stored_len;
    let crc = crc32c::crc32c(&block[..len]);
    block[56..60].copy_from_slice(&crc.to_le_bytes());
    Ok(len)
}

pub fn write_record(out: &mut impl Write, header: RecordHeader, payload: &[u8]) -> Result<()> {
    let mut buffer = vec![0; HEADER_LEN + payload.len()];
    buffer[HEADER_LEN..].copy_from_slice(payload);
    let n = seal(&mut buffer, header, payload.len(), payload.len(), 0)?;
    out.write_all(&buffer[..n])?;
    Ok(())
}

pub fn metadata_file_payload(path: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        path.len() <= 4096 && 4 + path.len() + bytes.len() <= MAX_PAYLOAD,
        "metadata file too large"
    );
    let mut data = Vec::with_capacity(4 + path.len() + bytes.len());
    data.extend_from_slice(&(path.len() as u32).to_le_bytes());
    data.extend_from_slice(path.as_bytes());
    data.extend_from_slice(bytes);
    Ok(data)
}
