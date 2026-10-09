//! Datagram trigger clients, with no tracing, storage, or export dependencies.
//!
//! Critical callers should prepare a request and destination during initialization,
//! then attempt one nonblocking send. Callers own the client lifetime and its
//! source label explicitly. No client waits for a reply.
//!
//! ```no_run
//! use retroft_trigger::{NonblockingClient, TrySendError};
//! use std::path::Path;
//!
//! # fn example() -> anyhow::Result<()> {
//! // Noncritical setup: encoding allocates and prepares the complete datagram.
//! let mut client = NonblockingClient::new(Path::new("/run/retroft.sock"), "motor-control")?;
//! let prepared = client.prepare("control deadline missed", None)?;
//!
//! // Critical path: one send, no userspace allocation, retries, or logging.
//! match client.try_send(&prepared) {
//!     Ok(()) => { /* Locally queued; acceptance remains unknown. */ }
//!     Err(TrySendError::WouldBlock | TrySendError::Interrupted) => {
//!         // No datagram queued; save a small status for later handling.
//!     }
//!     Err(TrySendError::Socket(_)) => { /* Handle outside the critical path. */ }
//! }
//! # Ok(())
//! # }
//! ```
use anyhow::{ensure, Context, Result};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixDatagram;
use std::path::Path;

const MAX_DESCRIPTION: usize = 1024;
const MAX_SOURCE: usize = 256;
const MAX_MESSAGE: usize = 2048;

/// CLOCK_MONOTONIC nanoseconds, the recorder's endpoint clock domain.
pub fn monotonic_ns() -> Result<u64> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: timestamp points to a valid writable timespec.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) } != 0 {
        return Err(std::io::Error::last_os_error()).context("read monotonic clock");
    }
    (timestamp.tv_sec as u64)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(timestamp.tv_nsec as u64))
        .context("monotonic clock overflow")
}

/// A bounded JSON datagram made by [`NonblockingClient::prepare`] outside the critical path.
/// The source is a human label, not an authenticated identity. The optional
/// endpoint is absolute; use `None` to reuse a receipt-time capture request.
pub struct PreparedTrigger {
    bytes: Box<[u8]>,
}
/// Outcome of one nonblocking datagram send; no error poisons framing.
#[derive(Debug)]
pub enum TrySendError {
    /// No datagram queued; socket capacity was unavailable.
    WouldBlock,
    /// No datagram queued; interrupted before transmission, without retry.
    Interrupted,
    Socket(std::io::Error),
}
impl std::fmt::Display for TrySendError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WouldBlock => output.write_str("trigger socket would block; request not queued"),
            Self::Interrupted => output.write_str("trigger send interrupted; request not queued"),
            Self::Socket(error) => write!(output, "trigger socket failed: {error}"),
        }
    }
}
impl std::error::Error for TrySendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Socket(error) => Some(error),
            _ => None,
        }
    }
}

/// Unconnected datagram client with its destination encoded during initialization.
/// `try_send` makes no userspace allocation, capacity wait, or automatic retry.
/// Reusing the destination path allows future sends after the daemon rebinds it.
pub struct NonblockingClient {
    socket: UnixDatagram,
    source: Box<str>,
    address: libc::sockaddr_un,
    address_len: libc::socklen_t,
}
impl NonblockingClient {
    /// Prepare outside the critical path. The recorder need not be running yet.
    pub fn new(path: &Path, source: &str) -> std::io::Result<Self> {
        if source.is_empty() || source.len() > MAX_SOURCE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "source must contain 1 through 256 bytes",
            ));
        }
        // SAFETY: zero initializes the address padding and pathname terminator.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        let path = path.as_os_str().as_bytes();
        if path.is_empty() || path.contains(&0) || path.len() >= address.sun_path.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid Unix socket path",
            ));
        }
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (target, &source) in address.sun_path.iter_mut().zip(path) {
            *target = source as libc::c_char;
        }
        let address_len =
            (std::mem::offset_of!(libc::sockaddr_un, sun_path) + path.len() + 1) as libc::socklen_t;
        let socket = UnixDatagram::unbound()?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            source: source.into(),
            address,
            address_len,
        })
    }

    /// Encode a request with this client's source label. This allocates and must
    /// run outside critical code; the resulting request can be sent repeatedly.
    /// Its endpoint, if supplied, is an absolute timestamp fixed at preparation.
    pub fn prepare(
        &self,
        description: &str,
        end_timestamp_ns: Option<u64>,
    ) -> Result<PreparedTrigger> {
        ensure!(
            description.len() <= MAX_DESCRIPTION,
            "description exceeds 1024 bytes"
        );
        let request = serde_json::json!({
            "description": description,
            "source": &self.source,
            "end_timestamp_ns": end_timestamp_ns.map(|value| value.to_string()),
        });
        let bytes = serde_json::to_vec(&request)?;
        ensure!(
            bytes.len() <= MAX_MESSAGE,
            "escaped trigger exceeds protocol size"
        );
        Ok(PreparedTrigger {
            bytes: bytes.into_boxed_slice(),
        })
    }

    /// Send one complete datagram or none. Success means local queuing only.
    /// Never retries, logs, closes the socket, or waits for capacity/a reply.
    pub fn try_send(&mut self, trigger: &PreparedTrigger) -> std::result::Result<(), TrySendError> {
        // SAFETY: the socket and complete readable payload remain valid here.
        let sent = unsafe {
            libc::sendto(
                self.socket.as_raw_fd(),
                trigger.bytes.as_ptr().cast(),
                trigger.bytes.len(),
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                (&self.address as *const libc::sockaddr_un).cast(),
                self.address_len,
            )
        };
        if sent < 0 {
            let error = std::io::Error::last_os_error();
            return Err(match error.kind() {
                std::io::ErrorKind::WouldBlock => TrySendError::WouldBlock,
                std::io::ErrorKind::Interrupted => TrySendError::Interrupted,
                _ => TrySendError::Socket(error),
            });
        }
        // Unix datagrams are atomic; a short result would violate that contract.
        if sent as usize != trigger.bytes.len() {
            return Err(TrySendError::Socket(std::io::ErrorKind::WriteZero.into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> Result<(tempfile::TempDir, std::path::PathBuf, UnixDatagram)> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("trigger.sock");
        let socket = UnixDatagram::bind(&path)?;
        socket.set_read_timeout(Some(std::time::Duration::from_secs(1)))?;
        Ok((directory, path, socket))
    }

    #[test]
    fn persistent_client_sends_separate_complete_datagrams() -> Result<()> {
        let (_directory, path, peer) = endpoint()?;
        let mut client = NonblockingClient::new(&path, "test")?;
        let prepared = client.prepare("line\nquote\"", Some(9_007_199_254_740_999))?;
        client.try_send(&prepared)?;
        client.try_send(&prepared)?;
        let mut bytes = [0u8; MAX_MESSAGE];
        for _ in 0..2 {
            let len = peer.recv(&mut bytes)?;
            let value: serde_json::Value = serde_json::from_slice(&bytes[..len])?;
            assert_eq!(value["description"], "line\nquote\"");
            assert_eq!(value["end_timestamp_ns"], "9007199254740999");
            assert_eq!(value["source"], "test");
            assert!(value.get("ack").is_none());
            assert_ne!(bytes[len - 1], b'\n');
        }
        Ok(())
    }

    #[test]
    fn backpressure_sends_whole_datagrams_and_recovers() -> Result<()> {
        let (_directory, path, peer) = endpoint()?;
        let mut client = NonblockingClient::new(&path, "test")?;
        let prepared = client.prepare("pressure", None)?;
        let mut count = 0;
        loop {
            match client.try_send(&prepared) {
                Ok(()) => count += 1,
                Err(TrySendError::WouldBlock) => break,
                other => panic!("unexpected pressure outcome: {other:?}"),
            }
            assert!(count < 10_000);
        }
        assert!(count > 0);
        assert!(matches!(
            client.try_send(&prepared),
            Err(TrySendError::WouldBlock)
        ));
        let mut bytes = [0u8; MAX_MESSAGE];
        for _ in 0..count {
            let len = peer.recv(&mut bytes)?;
            assert_eq!(&bytes[..len], prepared.bytes.as_ref());
        }
        client.try_send(&prepared)?;
        Ok(())
    }

    #[test]
    fn client_owns_source_and_applies_it_to_each_prepared_request() -> Result<()> {
        let (_directory, path, server) = endpoint()?;
        let mut client = {
            let source = String::from("service-\"worker\"");
            NonblockingClient::new(&path, &source)?
        };
        let mut bytes = [0; MAX_MESSAGE];
        for description in ["first", "second"] {
            let request = client.prepare(description, None)?;
            client.try_send(&request)?;
            let len = server.recv(&mut bytes)?;
            let value: serde_json::Value = serde_json::from_slice(&bytes[..len])?;
            assert_eq!(value["source"], "service-\"worker\"");
            assert_eq!(value["description"], description);
        }
        Ok(())
    }

    #[test]
    fn client_survives_absent_endpoint_and_rebinding_without_reconnect() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("trigger.sock");
        let mut client = NonblockingClient::new(&path, "test")?;
        let frame = client.prepare("restart", None)?;
        assert!(matches!(
            client.try_send(&frame),
            Err(TrySendError::Socket(_))
        ));
        let mut bytes = [0u8; MAX_MESSAGE];
        for _ in 0..2 {
            let server = UnixDatagram::bind(&path)?;
            server.set_read_timeout(Some(std::time::Duration::from_secs(1)))?;
            client.try_send(&frame)?;
            let len = server.recv(&mut bytes)?;
            assert_eq!(&bytes[..len], frame.bytes.as_ref());
            drop(server);
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    #[test]
    fn initialization_checks_utf8_and_encoded_limits_before_send() -> Result<()> {
        let path = Path::new("/absent");
        let client = NonblockingClient::new(path, &"é".repeat(128))?;
        let frame = client.prepare("test", None)?;
        let value: serde_json::Value = serde_json::from_slice(&frame.bytes)?;
        assert_eq!(value["source"].as_str().unwrap().len(), 256);
        assert!(NonblockingClient::new(path, "").is_err());
        assert!(NonblockingClient::new(path, &"é".repeat(129)).is_err());
        assert!(client.prepare(&"x".repeat(1025), None).is_err());
        assert!(client.prepare(&"\n".repeat(1024), None).is_err());
        assert!(NonblockingClient::new(Path::new(""), "test").is_err());
        Ok(())
    }
}
