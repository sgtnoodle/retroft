//! Single-threaded integration with a stand-in for a message bus.
//! Run only on a recording target; see docs/embedding.md for setup and protocol.
use anyhow::{ensure, Context, Result};
use retroft::{
    daemon::{Config, Recorder, Step},
    trigger::{self, TriggerRequest},
    Shutdown, StatusEvent,
};
use serde::Deserialize;
use std::{
    io,
    os::{
        fd::{AsFd, AsRawFd},
        unix::net::UnixDatagram,
    },
    path::PathBuf,
    time::Duration,
};

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Trigger {
        source: String,
        description: String,
        end_timestamp_ns: Option<u64>,
    },
    Shutdown,
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    ensure!(
        args.len() == 3,
        "usage: embedded_loop OUTPUT_DIR COMMAND_SOCKET STATUS_SOCKET"
    );
    // The demo uses an application-owned datagram socket as a message bus.
    // Bind refuses an existing path; the deployment owns pathname cleanup.
    let bus = UnixDatagram::bind(&args[1])?;
    bus.set_nonblocking(true)?;
    let publisher = UnixDatagram::unbound()?;
    publisher.set_nonblocking(true)?;
    let mut dropped_status = 0u64;
    let mut status = |event: StatusEvent| {
        // No status queue or worker thread: send once, drop on backpressure.
        // A real integration selects its own schema, authentication and policy.
        let sent = serde_json::to_vec(&event)
            .ok()
            .is_some_and(|bytes| publisher.send_to(&bytes, &args[2]).is_ok());
        if !sent {
            dropped_status = dropped_status.saturating_add(1);
        }
    };
    let shutdown = Shutdown::new()?;
    // This small queue lets the application service its bus (including shutdown)
    // even while recorder trigger admission is paused during a frozen-bank drain.
    let (triggers, input) = trigger::channel(16)?;
    let mut config = Config::new(PathBuf::from(&args[0]));
    config.idle_report = Some(Duration::from_secs(5));
    let mut recorder = Recorder::new(config, shutdown.clone(), input, &mut status)?;
    let mut bytes = [0u8; 4097];
    loop {
        // Bound bus work too, so a busy publisher cannot starve trace draining.
        for _ in 0..16 {
            match bus.recv(&mut bytes) {
                Ok(count) if count <= 4096 => match serde_json::from_slice(&bytes[..count]) {
                    Ok(Command::Trigger {
                        source,
                        description,
                        end_timestamp_ns,
                    }) => {
                        if let Err(error) = triggers.try_send(TriggerRequest::new(
                            source,
                            description,
                            end_timestamp_ns,
                        )) {
                            eprintln!("trigger not queued: {error}");
                        }
                    }
                    Ok(Command::Shutdown) => shutdown.request(),
                    Err(error) => eprintln!("invalid bus command: {error}"),
                },
                Ok(_) => eprintln!("oversized bus command discarded"),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        if recorder.step(&mut status)? == Step::Finished {
            break;
        }
        // Register just two sources: the application's bus and Retroft itself.
        // Retroft stays readable while immediate work remains, so yielding here
        // does not lose its wakeup. Its internal timer handles all deadlines.
        let mut fds = [
            pollfd(bus.as_raw_fd()),
            pollfd(recorder.as_fd().as_raw_fd()),
        ];
        // SAFETY: both descriptor owners and the pollfd array outlive poll.
        if unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, -1) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error).context("wait for bus or recorder");
            }
        }
        ensure!(
            fds.iter()
                .all(|fd| fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) == 0),
            "poll descriptor failed"
        );
    }
    eprintln!("status messages dropped: {dropped_status}");
    Ok(())
}
fn pollfd(fd: i32) -> libc::pollfd {
    libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    }
}
