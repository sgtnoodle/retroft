//! Nonblocking trigger sources, including Unix sockets and an in-process queue.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd},
        unix::{fs::PermissionsExt, net::UnixDatagram},
    },
    path::{Path, PathBuf},
};
pub(crate) const MAX_DESCRIPTION: usize = 1024;
pub(crate) const MAX_SOURCE: usize = 256;
pub(crate) const MAX_MESSAGE: usize = 2048;

/// A source supplies a readable descriptor and consumes at most one request per
/// call. `receive` must never block. Empty means no work; Ignored means one bad
/// message was consumed. Errors terminate the recorder. Readiness must remain
/// asserted while requests are queued. The recorder controls admission timing.
pub trait TriggerSource: AsFd {
    fn receive(&mut self) -> Result<TriggerRead>;
}
pub enum TriggerRead {
    Empty,
    Ignored,
    Request(TriggerRequest),
}

/// Source and description are UTF-8 labels, bounded to 256 and 1024 bytes.
/// The endpoint is absolute CLOCK_MONOTONIC nanoseconds, or None for admission
/// time. Sequence numbers and occurrence timestamps are assigned by the recorder.
#[derive(Debug)]
#[non_exhaustive]
pub struct TriggerRequest {
    pub source: String,
    pub description: String,
    pub end_timestamp_ns: Option<u64>,
    pub(crate) peer: Option<PeerIdentity>,
}
impl TriggerRequest {
    pub fn new(source: String, description: String, end_timestamp_ns: Option<u64>) -> Self {
        Self {
            source,
            description,
            end_timestamp_ns,
            peer: None,
        }
    }
    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() <= MAX_MESSAGE, "trigger exceeds message limit");
        let request: Request = serde_json::from_slice(bytes).context("invalid trigger JSON")?;
        Ok(Self {
            source: request.source.0.context("trigger requires source")?,
            description: request.description.0.unwrap_or_default(),
            end_timestamp_ns: request
                .end_timestamp_ns
                .0
                .as_deref()
                .map(parse_endpoint)
                .transpose()?,
            peer: None,
        })
    }
}
// A wrapper distinguishes a present null from a missing field, so serde also
// rejects duplicate nullable fields. Request strings may allocate;
// large trace buffers and compression storage remain fixed.
#[derive(Default, Deserialize)]
#[serde(transparent)]
struct NullableString(Option<String>);

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(default)]
    description: NullableString,
    #[serde(default)]
    source: NullableString,
    #[serde(default)]
    end_timestamp_ns: NullableString,
}

fn parse_endpoint(value: &str) -> Result<u64> {
    ensure!(
        !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && (value == "0" || !value.starts_with('0')),
        "end_timestamp_ns must be canonical decimal nanoseconds"
    );
    value.parse().context("end_timestamp_ns exceeds u64")
}

// Credentials accompany each datagram, so inherited sockets still identify the
// sender of that message. Names are diagnostic labels sampled after receipt.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PeerIdentity {
    pub(crate) peer_pid: i32,
    pub(crate) peer_uid: u32,
    pub(crate) peer_gid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) peer_process_name: Option<String>,
}
impl PeerIdentity {
    fn from_credentials(credentials: libc::ucred) -> Self {
        let peer_process_name = (|| -> std::io::Result<String> {
            let file = File::open(format!("/proc/{}/comm", credentials.pid))?;
            let mut bytes = Vec::with_capacity(64);
            file.take(64).read_to_end(&mut bytes)?;
            if bytes.last() == Some(&b'\n') {
                bytes.pop();
            }
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        })()
        .ok();
        Self {
            peer_pid: credentials.pid,
            peer_uid: credentials.uid,
            peer_gid: credentials.gid,
            peer_process_name,
        }
    }
}

pub struct UnixSocket {
    datagram: UnixDatagram,
    // Only standalone sockets own their pathname; systemd keeps its socket alive.
    owned_path: Option<PathBuf>,
    identity: Option<(u64, u64)>,
    // Keep the lock inode permanently; unlinking it permits competing lock owners.
    _lock: Option<File>,
}
impl UnixSocket {
    pub fn open(path: &Path, mode: u32) -> Result<Self> {
        if let Some(datagram) = crate::activation::take_socket()? {
            ensure!(
                datagram.local_addr()?.as_pathname() == Some(path),
                "activated socket path differs from --socket"
            );
            return Ok(Self {
                datagram,
                owned_path: None,
                identity: None,
                _lock: None,
            });
        }
        Self::bind(path, mode)
    }
    pub fn bind(path: &Path, mode: u32) -> Result<Self> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
        ensure!(mode & !0o777 == 0, "invalid socket mode");
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(PathBuf::from(lock_path))
            .context("open socket ownership lock")?;
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "another recorder owns the trigger socket"
        );
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                ensure!(
                    metadata.file_type().is_socket(),
                    "existing trigger path is not a socket"
                );
                // Do not replace a live socket, including one owned by another tool.
                let probe = UnixDatagram::unbound()?;
                match probe.connect(path) {
                    Err(error) if error.raw_os_error() == Some(libc::ECONNREFUSED) => {}
                    _ => anyhow::bail!(
                        "existing trigger socket is live or cannot be safely reclaimed"
                    ),
                }
                let current = fs::symlink_metadata(path)?;
                ensure!(
                    (current.dev(), current.ino()) == (metadata.dev(), metadata.ino()),
                    "trigger socket changed during stale-path check"
                );
                fs::remove_file(path).context("remove stale trigger socket")?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        // Enable credentials before publishing the pathname: otherwise an early
        // datagram can be queued without its sender identity.
        use std::os::unix::ffi::OsStrExt;
        let datagram = UnixDatagram::unbound()?;
        crate::activation::configure_socket(&datagram)?;
        let name = path.as_os_str().as_bytes();
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        ensure!(
            !name.is_empty() && name.len() < address.sun_path.len() && !name.contains(&0),
            "invalid Unix socket pathname"
        );
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (slot, byte) in address.sun_path.iter_mut().zip(name) {
            *slot = *byte as libc::c_char;
        }
        let length =
            (std::mem::offset_of!(libc::sockaddr_un, sun_path) + name.len() + 1) as libc::socklen_t;
        // SAFETY: initialized sockaddr_un contains a bounded NUL-terminated path.
        if unsafe {
            libc::bind(
                datagram.as_raw_fd(),
                (&address as *const libc::sockaddr_un).cast(),
                length,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("bind {}", path.display()));
        }
        let metadata = fs::symlink_metadata(path)?;
        let socket = Self {
            datagram,
            owned_path: Some(path.into()),
            identity: Some((metadata.dev(), metadata.ino())),
            _lock: Some(lock),
        };
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
        Ok(socket)
    }
}
impl AsFd for UnixSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.datagram.as_fd()
    }
}
impl TriggerSource for UnixSocket {
    fn receive(&mut self) -> Result<TriggerRead> {
        let mut bytes = [0u8; MAX_MESSAGE];
        // usize storage supplies cmsghdr alignment on each supported Linux ABI.
        let mut control = [0usize; 64];
        let mut io = libc::iovec {
            iov_base: bytes.as_mut_ptr().cast(),
            iov_len: bytes.len(),
        };
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut io;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = std::mem::size_of_val(&control);
        // SAFETY: all buffers are exclusive, aligned, and live through recvmsg.
        let count = unsafe {
            libc::recvmsg(
                self.datagram.as_raw_fd(),
                &mut message,
                libc::MSG_DONTWAIT | libc::MSG_CMSG_CLOEXEC,
            )
        };
        if count < 0 {
            let error = std::io::Error::last_os_error();
            return if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
            ) {
                Ok(TriggerRead::Empty)
            } else {
                Err(error.into())
            };
        }
        let mut credentials = None;
        let mut unexpected_control = false;
        // Always close received descriptors, even when ancillary data is truncated.
        // The protocol never accepts descriptor passing from a trigger client.
        unsafe {
            let mut header = libc::CMSG_FIRSTHDR(&message);
            while !header.is_null() {
                let len = (*header)
                    .cmsg_len
                    .saturating_sub(libc::CMSG_LEN(0) as usize);
                if (*header).cmsg_level == libc::SOL_SOCKET
                    && (*header).cmsg_type == libc::SCM_CREDENTIALS
                    && len == std::mem::size_of::<libc::ucred>()
                {
                    credentials = Some(std::ptr::read_unaligned(
                        libc::CMSG_DATA(header).cast::<libc::ucred>(),
                    ));
                } else {
                    unexpected_control = true;
                    if (*header).cmsg_level == libc::SOL_SOCKET
                        && (*header).cmsg_type == libc::SCM_RIGHTS
                    {
                        for i in 0..len / std::mem::size_of::<libc::c_int>() {
                            let fd = std::ptr::read_unaligned(
                                libc::CMSG_DATA(header).cast::<libc::c_int>().add(i),
                            );
                            libc::close(fd);
                        }
                    }
                }
                header = libc::CMSG_NXTHDR(&message, header);
            }
        }
        if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 || unexpected_control {
            return Ok(TriggerRead::Ignored); // Consumed an invalid message; do not interpret a prefix.
        }
        let Some(credentials) = credentials else {
            return Ok(TriggerRead::Ignored);
        };
        let Ok(mut request) = TriggerRequest::from_json(&bytes[..count as usize]) else {
            return Ok(TriggerRead::Ignored);
        };
        request.peer = Some(PeerIdentity::from_credentials(credentials));
        Ok(TriggerRead::Request(request))
    }
}
impl Drop for UnixSocket {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if let Some(path) = &self.owned_path {
            if fs::symlink_metadata(path).ok().map(|m| (m.dev(), m.ino())) == self.identity {
                let _ = fs::remove_file(path);
            }
        }
    }
}

/// Optional bridge from a bus/event-loop thread into the recorder. Capacity is
/// measured in requests; Full and Disconnected are reported without waiting.
/// Size limits are checked again at recorder admission. Enqueueing is not an
/// acknowledgment of acceptance; observe CaptureStarted/TriggerAdded instead.
#[derive(Clone)]
pub struct TriggerSender {
    sender: std::sync::mpsc::SyncSender<TriggerRequest>,
    wake: std::sync::Arc<crate::readiness::Wake>,
}
pub struct TriggerReceiver {
    receiver: std::sync::mpsc::Receiver<TriggerRequest>,
    wake: std::sync::Arc<crate::readiness::Wake>,
}
/// Closing all senders does not shut down the recorder. Use Shutdown explicitly.
pub fn channel(capacity: usize) -> std::io::Result<(TriggerSender, TriggerReceiver)> {
    if capacity == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "trigger capacity must be positive",
        ));
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
    let wake = std::sync::Arc::new(crate::readiness::Wake::new()?);
    Ok((
        TriggerSender {
            sender,
            wake: wake.clone(),
        },
        TriggerReceiver { receiver, wake },
    ))
}
impl TriggerSender {
    pub fn try_send(
        &self,
        request: TriggerRequest,
    ) -> std::result::Result<(), std::sync::mpsc::TrySendError<TriggerRequest>> {
        self.sender.try_send(request)?;
        self.wake.notify();
        Ok(())
    }
}
impl AsFd for TriggerReceiver {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.wake.as_fd()
    }
}
impl TriggerSource for TriggerReceiver {
    fn receive(&mut self) -> Result<TriggerRead> {
        if let Ok(request) = self.receiver.try_recv() {
            return Ok(TriggerRead::Request(request));
        }
        // Clear only when empty, so stopping admission with queued requests
        // leaves the descriptor readable. Recheck in case a sender queued a request
        // between the empty check and clearing the notification.
        self.wake.clear()?;
        if let Ok(request) = self.receiver.try_recv() {
            self.wake.notify();
            Ok(TriggerRead::Request(request))
        } else {
            Ok(TriggerRead::Empty)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn readable(source: &impl AsFd) -> bool {
        let mut fd = libc::pollfd {
            fd: source.as_fd().as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: descriptor is borrowed from a live owner; one initialized pollfd.
        assert!(unsafe { libc::poll(&mut fd, 1, 0) } >= 0);
        fd.revents & libc::POLLIN != 0
    }
    fn request(n: usize) -> TriggerRequest {
        TriggerRequest::new("test".into(), n.to_string(), None)
    }
    #[test]
    fn bounded_channel_preserves_readiness_and_reports_backpressure() -> Result<()> {
        assert!(channel(0).is_err());
        let (sender, mut receiver) = channel(2)?;
        assert!(!readable(&receiver));
        sender.try_send(request(1))?;
        sender.try_send(request(2))?;
        assert!(matches!(
            sender.try_send(request(3)),
            Err(std::sync::mpsc::TrySendError::Full(_))
        ));
        for n in [1, 2] {
            assert!(readable(&receiver));
            let TriggerRead::Request(message) = receiver.receive()? else {
                panic!("missing request")
            };
            assert_eq!(message.description, n.to_string());
        }
        assert!(matches!(receiver.receive()?, TriggerRead::Empty));
        assert!(!readable(&receiver));
        drop(receiver);
        assert!(matches!(
            sender.try_send(request(4)),
            Err(std::sync::mpsc::TrySendError::Disconnected(_))
        ));
        Ok(())
    }
    #[test]
    fn concurrent_enqueue_and_empty_checks_do_not_lose_wakeups() -> Result<()> {
        let (sender, mut receiver) = channel(8)?;
        let worker = std::thread::spawn(move || {
            for n in 0..1000 {
                let mut pending = request(n);
                loop {
                    match sender.try_send(pending) {
                        Ok(()) => break,
                        Err(std::sync::mpsc::TrySendError::Full(value)) => {
                            pending = value;
                            std::thread::yield_now();
                        }
                        Err(error) => panic!("{error}"),
                    }
                }
            }
        });
        for n in 0..1000 {
            loop {
                match receiver.receive()? {
                    TriggerRead::Request(value) => {
                        assert_eq!(value.description, n.to_string());
                        break;
                    }
                    TriggerRead::Empty => {
                        let mut fd = libc::pollfd {
                            fd: receiver.as_fd().as_raw_fd(),
                            events: libc::POLLIN,
                            revents: 0,
                        };
                        // SAFETY: the receiver and initialized pollfd outlive poll.
                        assert!(unsafe { libc::poll(&mut fd, 1, 2000) } > 0, "missed wakeup");
                    }
                    TriggerRead::Ignored => panic!("typed queue cannot discard requests"),
                }
            }
        }
        worker.join().unwrap();
        assert!(matches!(receiver.receive()?, TriggerRead::Empty));
        assert!(!readable(&receiver));
        Ok(())
    }
}
