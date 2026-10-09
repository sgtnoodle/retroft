//! Minimal systemd socket activation for the recorder's Unix datagram socket.
use anyhow::{ensure, Context, Result};
use std::{
    os::fd::{AsRawFd, FromRawFd, RawFd},
    os::unix::net::UnixDatagram,
};

const ACTIVATED_FD: RawFd = 3;

/// Takes the single socket inherited from systemd, if this process was activated.
/// The caller must also check its pathname against the configured socket path.
pub fn take_socket() -> Result<Option<UnixDatagram>> {
    let pid = std::env::var("LISTEN_PID").ok();
    let fds = std::env::var("LISTEN_FDS").ok();
    // Do not accidentally pass activation metadata to a future child process.
    for name in ["LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"] {
        std::env::remove_var(name);
    }
    let Some(pid) = pid else {
        ensure!(fds.is_none(), "LISTEN_FDS provided without LISTEN_PID");
        return Ok(None);
    };
    let pid: u32 = pid.parse().context("invalid LISTEN_PID")?;
    if pid != std::process::id() {
        return Ok(None);
    }
    ensure!(
        fds.as_deref() == Some("1"),
        "socket activation requires exactly one descriptor"
    );
    take_descriptor(ACTIVATED_FD).map(Some)
}

/// Signal readiness only after tracing, storage, and the trigger socket are
/// usable. Direct invocation has no NOTIFY_SOCKET and needs no service manager.
pub fn notify_ready() -> Result<()> {
    let Some(address) = std::env::var_os("NOTIFY_SOCKET") else {
        return Ok(());
    };
    notify_address(&address)
}

fn notify_address(address: &std::ffi::OsStr) -> Result<()> {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::{ffi::OsStrExt, net::SocketAddr};
    let bytes = address.as_bytes();
    let destination = if bytes.starts_with(b"@") {
        ensure!(bytes.len() > 1, "empty abstract NOTIFY_SOCKET");
        SocketAddr::from_abstract_name(&bytes[1..])?
    } else {
        ensure!(
            bytes.starts_with(b"/"),
            "NOTIFY_SOCKET must be absolute or abstract"
        );
        SocketAddr::from_pathname(address)?
    };
    let socket = UnixDatagram::unbound().context("create readiness notification socket")?;
    socket.set_nonblocking(true)?;
    socket
        .connect_addr(&destination)
        .context("connect readiness notification socket")?;
    let message = b"READY=1\nSTATUS=Collecting ftrace history";
    let sent = socket
        .send(message)
        .context("send readiness notification")?;
    ensure!(sent == message.len(), "incomplete readiness notification");
    Ok(())
}

fn take_descriptor(fd: RawFd) -> Result<UnixDatagram> {
    let mut kind: libc::c_int = 0;
    let mut length = std::mem::size_of_val(&kind) as libc::socklen_t;
    // Validate before assuming ownership of an arbitrary inherited descriptor.
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut kind as *mut libc::c_int).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("inspect activated socket");
    }
    ensure!(
        kind == libc::SOCK_DGRAM,
        "activated socket must be datagram"
    );
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of_val(&address) as libc::socklen_t;
    let result = unsafe {
        libc::getsockname(
            fd,
            (&mut address as *mut libc::sockaddr_un).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("inspect activated address");
    }
    ensure!(
        address.sun_family == libc::AF_UNIX as libc::sa_family_t,
        "activated socket must use AF_UNIX"
    );
    ensure!(
        length as usize > std::mem::size_of::<libc::sa_family_t>(),
        "activated socket must be bound"
    );
    // SAFETY: this process exclusively owns the validated activation descriptor.
    let socket = unsafe { UnixDatagram::from_raw_fd(fd) };
    configure_socket(&socket)?;
    Ok(socket)
}

/// Shared setup for inherited and directly bound sockets. Credential delivery
/// must be enabled before requests arrive to preserve each datagram's sender.
pub fn configure_socket(socket: &UnixDatagram) -> Result<()> {
    socket.set_nonblocking(true)?;
    let fd = socket.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error()).context("set socket close-on-exec");
    }
    let enabled: libc::c_int = 1;
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PASSCRED,
            (&enabled as *const libc::c_int).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("enable datagram sender credentials");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::IntoRawFd;
    use std::os::unix::net::UnixListener;

    #[test]
    fn readiness_notification_supports_pathname_and_abstract_addresses() -> Result<()> {
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::net::SocketAddr;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("notify.sock");
        let pathname = UnixDatagram::bind(&path)?;
        let abstract_name = format!("retroft-notify-test-{}", std::process::id());
        let abstract_address = SocketAddr::from_abstract_name(abstract_name.as_bytes())?;
        let abstract_socket = UnixDatagram::bind_addr(&abstract_address)?;
        let abstract_env = format!("@{abstract_name}");
        for (receiver, address) in [
            (&pathname, path.as_os_str()),
            (&abstract_socket, std::ffi::OsStr::new(&abstract_env)),
        ] {
            receiver.set_read_timeout(Some(std::time::Duration::from_secs(1)))?;
            // Repeated sends use temporary descriptors, never retained in state.
            for _ in 0..3 {
                notify_address(address)?;
                let mut message = [0; 128];
                let len = receiver.recv(&mut message)?;
                assert_eq!(
                    &message[..len],
                    b"READY=1\nSTATUS=Collecting ftrace history"
                );
            }
        }
        assert!(notify_address(std::ffi::OsStr::new("relative/socket")).is_err());
        assert!(notify_address(std::ffi::OsStr::new("@")).is_err());
        Ok(())
    }

    #[test]
    fn accepts_bound_datagram_and_preserves_queued_request() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("activated.sock");
        let original = UnixDatagram::bind(&path)?;
        configure_socket(&original)?;
        let sender = UnixDatagram::unbound()?;
        sender.send_to(b"queued before daemon takes ownership", &path)?;
        let socket = take_descriptor(original.into_raw_fd())?;
        let mut bytes = [0; 128];
        let count = socket.recv(&mut bytes)?;
        assert_eq!(&bytes[..count], b"queued before daemon takes ownership");
        assert_eq!(socket.local_addr()?.as_pathname(), Some(path.as_path()));
        assert_eq!(
            unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            libc::FD_CLOEXEC
        );
        assert_eq!(
            socket.recv(&mut bytes).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        Ok(())
    }

    #[test]
    fn rejects_stream_and_unbound_datagram_without_taking_ownership() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let listener = UnixListener::bind(directory.path().join("stream.sock"))?;
        assert!(take_descriptor(listener.as_raw_fd()).is_err());
        assert!(listener.local_addr().is_ok());
        let socket = UnixDatagram::unbound()?;
        assert!(take_descriptor(socket.as_raw_fd()).is_err());
        assert!(socket.local_addr()?.is_unnamed());
        Ok(())
    }
}
