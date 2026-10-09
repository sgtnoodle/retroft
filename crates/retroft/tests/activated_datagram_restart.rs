//! Verify the kernel queue lifetime used by socket activation without systemd.
use std::{
    io,
    os::{
        fd::AsRawFd,
        unix::{net::UnixDatagram, process::CommandExt},
    },
    process::Command,
};

#[test]
fn activation_child() {
    let Ok(mode) = std::env::var("RETROFT_ACTIVATION_TEST_CHILD") else {
        return;
    };
    // This is a separately executed test process, not a mutation of the parent
    // test runner's activation environment.
    std::env::set_var("LISTEN_PID", std::process::id().to_string());
    let socket = retroft::activation::take_socket()
        .expect("valid inherited datagram socket")
        .expect("activation socket present");
    if mode == "crash" {
        // Bypass Rust destructors to model an abrupt service process failure.
        std::process::exit(73);
    }
    let mut bytes = [0; 128];
    let first = socket.recv(&mut bytes).expect("queued before crash");
    assert_eq!(&bytes[..first], b"queued before crash");
    let second = socket.recv(&mut bytes).expect("same sender after crash");
    assert_eq!(&bytes[..second], b"same sender after crash");
    assert_eq!(
        socket.recv(&mut bytes).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn unread_datagrams_and_connected_sender_survive_service_process_exit() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("trigger.sock");
    // This parent FD represents the socket unit, which outlives service children.
    let socket_unit = UnixDatagram::bind(&path)?;
    retroft::activation::configure_socket(&socket_unit).map_err(io::Error::other)?;
    let sender = UnixDatagram::unbound()?;
    sender.connect(&path)?;
    sender.set_nonblocking(true)?;
    sender.send(b"queued before crash")?;
    let run_service = |mode: &str| -> io::Result<std::process::Output> {
        let mut child = Command::new(std::env::current_exe()?);
        child
            .args(["--exact", "activation_child", "--nocapture"])
            .env("RETROFT_ACTIVATION_TEST_CHILD", mode)
            .env("LISTEN_FDS", "1")
            .env("LISTEN_FDNAMES", "trigger");
        let fd = socket_unit.as_raw_fd();
        // SAFETY: only async-signal-safe fd operations run between fork and exec.
        // The live parent socket remains open throughout child execution.
        unsafe {
            child.pre_exec(move || {
                if libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        child.output()
    };
    assert_eq!(run_service("crash")?.status.code(), Some(73));
    // No reconnect and no rebinding: the same client and listener remain usable.
    sender.send(b"same sender after crash")?;
    let restarted = run_service("receive")?;
    assert!(
        restarted.status.success(),
        "child failed: {}{}",
        String::from_utf8_lossy(&restarted.stdout),
        String::from_utf8_lossy(&restarted.stderr)
    );
    assert!(path.exists());
    Ok(())
}
