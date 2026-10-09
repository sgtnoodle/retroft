//! One pollable descriptor for recorder work and deadlines.
use anyhow::{ensure, Context, Result};
use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

const TIMER: u64 = 1;
const STORAGE: u64 = 2;
const SHUTDOWN: u64 = 3;
const TRIGGER: u64 = 4;
const RUNNABLE: u64 = 5;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deadline {
    Instant(Instant),
    Monotonic(u64),
}

pub(crate) struct Readiness {
    epoll: OwnedFd,
    timer: OwnedFd,
    runnable: Wake,
    runnable_set: bool,
    armed: Option<Deadline>,
    trigger: RawFd,
    trigger_enabled: bool,
    shutdown: RawFd,
    shutdown_enabled: bool,
}
impl Readiness {
    // Descriptor owners must outlive this object. Recorder owns them all.
    pub(crate) fn new(storage: RawFd, trigger: RawFd, shutdown: RawFd) -> Result<Self> {
        // SAFETY: scalar arguments; successful calls return newly owned descriptors.
        let epoll = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if epoll < 0 {
            return Err(io::Error::last_os_error()).context("create recorder epoll");
        }
        let epoll = unsafe { OwnedFd::from_raw_fd(epoll) };
        let timer = unsafe {
            libc::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
            )
        };
        if timer < 0 {
            return Err(io::Error::last_os_error()).context("create recorder timer");
        }
        let timer = unsafe { OwnedFd::from_raw_fd(timer) };
        let mut result = Self {
            epoll,
            timer,
            runnable: Wake::new()?,
            runnable_set: false,
            armed: None,
            trigger,
            trigger_enabled: false,
            shutdown,
            shutdown_enabled: false,
        };
        result.watch(result.timer.as_raw_fd(), TIMER, true)?;
        result.watch(storage, STORAGE, true)?;
        result.watch(result.runnable.fd(), RUNNABLE, true)?;
        result.update(true, true, true, None)?;
        Ok(result)
    }
    fn watch(&self, fd: RawFd, token: u64, enabled: bool) -> Result<()> {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: token,
        };
        // SAFETY: event is initialized; descriptor ownership is maintained by Recorder.
        let rc = unsafe {
            libc::epoll_ctl(
                self.epoll.as_raw_fd(),
                if enabled {
                    libc::EPOLL_CTL_ADD
                } else {
                    libc::EPOLL_CTL_DEL
                },
                fd,
                &mut event,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error()).context("update recorder readiness");
        }
        Ok(())
    }
    /// Keep the descriptor readable while immediate work remains. Draining does
    /// not rearm the timer or call epoll_wait for every sub-buffer.
    pub(crate) fn update(
        &mut self,
        runnable: bool,
        admit: bool,
        watch_shutdown: bool,
        deadline: Option<Deadline>,
    ) -> Result<()> {
        if admit != self.trigger_enabled {
            self.watch(self.trigger, TRIGGER, admit)?;
            self.trigger_enabled = admit;
        }
        if watch_shutdown != self.shutdown_enabled {
            self.watch(self.shutdown, SHUTDOWN, watch_shutdown)?;
            self.shutdown_enabled = watch_shutdown;
        }
        if runnable {
            if !self.runnable_set {
                self.runnable.notify();
                self.runnable_set = true;
            }
            return Ok(());
        }
        if self.runnable_set {
            self.runnable.clear()?;
            self.runnable_set = false;
        }
        // Refresh the epoll ready list and consume expired timer notifications
        // before arming the next deadline. Other sources are consumed by step.
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 5];
        let count = loop {
            // SAFETY: events has space for the requested initialized entries.
            let count = unsafe {
                libc::epoll_wait(
                    self.epoll.as_raw_fd(),
                    events.as_mut_ptr(),
                    events.len() as _,
                    0,
                )
            };
            if count >= 0 {
                break count as usize;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error).context("inspect recorder readiness");
            }
        };
        for event in &events[..count] {
            ensure!(
                event.events & (libc::EPOLLERR | libc::EPOLLHUP) as u32 == 0,
                "recorder readiness source failed"
            );
            if event.u64 == TIMER {
                let mut ticks = 0u64;
                loop {
                    // SAFETY: ticks supplies eight writable bytes and self owns timer.
                    let count = unsafe {
                        libc::read(self.timer.as_raw_fd(), (&mut ticks as *mut u64).cast(), 8)
                    };
                    if count >= 0 {
                        self.armed = None;
                        break;
                    }
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::WouldBlock {
                        break;
                    }
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error).context("read recorder timer");
                    }
                }
            }
        }
        if deadline == self.armed {
            return Ok(());
        }
        let ns = match deadline {
            None => 0,
            Some(Deadline::Monotonic(ns)) => ns.max(1),
            Some(Deadline::Instant(when)) => {
                // Bracket conversion conservatively: early wakeups are harmless.
                let mono = crate::clock::clock_ns(libc::CLOCK_MONOTONIC)?;
                let remaining = when.saturating_duration_since(Instant::now()).as_nanos();
                mono.saturating_add(u64::try_from(remaining)?).max(1)
            }
        };
        let spec = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: (ns / 1_000_000_000).try_into()?,
                tv_nsec: (ns % 1_000_000_000) as _,
            },
        };
        // SAFETY: spec is initialized and self owns the timer descriptor.
        if unsafe {
            libc::timerfd_settime(
                self.timer.as_raw_fd(),
                libc::TFD_TIMER_ABSTIME,
                &spec,
                std::ptr::null_mut(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error()).context("arm recorder deadline");
        }
        self.armed = deadline;
        Ok(())
    }
}
impl AsFd for Readiness {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.epoll.as_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn ready(fd: BorrowedFd<'_>, timeout: i32) -> bool {
        let mut item = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert!(unsafe { libc::poll(&mut item, 1, timeout) } >= 0);
        item.revents & libc::POLLIN != 0
    }
    #[test]
    fn runnable_and_paused_sources_work_in_a_parent_epoll() -> Result<()> {
        let storage = Wake::new()?;
        let trigger = Wake::new()?;
        let shutdown = Wake::new()?;
        let mut set = Readiness::new(storage.fd(), trigger.fd(), shutdown.fd())?;
        let parent = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        assert!(parent >= 0);
        let parent = unsafe { OwnedFd::from_raw_fd(parent) };
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: 1,
        };
        assert_eq!(
            unsafe {
                libc::epoll_ctl(
                    parent.as_raw_fd(),
                    libc::EPOLL_CTL_ADD,
                    set.as_fd().as_raw_fd(),
                    &mut event,
                )
            },
            0
        );
        for _ in 0..4 {
            assert!(ready(parent.as_fd(), 0));
            set.update(true, false, true, None)?;
        }
        set.update(false, false, true, None)?;
        assert!(!ready(parent.as_fd(), 0));
        trigger.notify();
        assert!(!ready(parent.as_fd(), 0));
        set.update(false, true, true, None)?;
        assert!(ready(parent.as_fd(), 0));
        trigger.clear()?;
        set.update(false, true, true, None)?;
        assert!(!ready(parent.as_fd(), 0));
        storage.notify();
        assert!(ready(parent.as_fd(), 0));
        storage.clear()?;
        shutdown.notify();
        set.update(false, false, false, None)?;
        assert!(!ready(parent.as_fd(), 0));
        Ok(())
    }
    #[test]
    fn deadlines_wake_once_and_shutdown_wakes_without_a_timer() -> Result<()> {
        let storage = Wake::new()?;
        let trigger = Wake::new()?;
        let shutdown = Wake::new()?;
        let mut set = Readiness::new(storage.fd(), trigger.fd(), shutdown.fd())?;
        let deadline = Deadline::Instant(Instant::now() + Duration::from_millis(30));
        set.update(false, false, true, Some(deadline))?;
        assert!(ready(set.as_fd(), 1000));
        set.update(false, false, true, None)?;
        assert!(!ready(set.as_fd(), 0));
        let sender = std::sync::Arc::new(shutdown);
        let worker = {
            let sender = sender.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                sender.notify();
            })
        };
        assert!(ready(set.as_fd(), 1000));
        worker.join().unwrap();
        Ok(())
    }
}

// Shared wakeup primitive for shutdown and the optional in-process trigger queue.
// Its descriptor stays owned while any sender can write to it.
pub(crate) struct Wake(OwnedFd);
impl Wake {
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: eventfd takes scalar arguments and returns an owned descriptor.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd was just created and has no other owner.
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd) }))
    }
    pub(crate) fn notify(&self) {
        let value = 1u64;
        loop {
            // SAFETY: the descriptor is held by self and value is eight readable bytes.
            let result = unsafe { libc::write(self.fd(), (&value as *const u64).cast(), 8) };
            // A saturated eventfd is already readable. Other errors cannot occur
            // for this privately owned, valid eventfd with an eight-byte write.
            if result >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
    }
    pub(crate) fn clear(&self) -> io::Result<()> {
        let mut value = 0u64;
        loop {
            // SAFETY: value supplies eight writable bytes; self owns the fd.
            let result = unsafe { libc::read(self.fd(), (&mut value as *mut u64).cast(), 8) };
            if result >= 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(error),
            }
        }
    }
    pub(crate) fn fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}
impl std::os::fd::AsFd for Wake {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}
struct State {
    requested: AtomicBool,
    wake: Wake,
}

/// Cloneable shutdown handle. A request cannot be cancelled; use a fresh handle
/// for each recorder run.
/// Requesting shutdown wakes the recorder, shortens any future capture endpoint,
/// and lets accepted data finish writing and syncing. It cannot interrupt a
/// blocked kernel operation. Dropping a handle does not request shutdown.
#[derive(Clone)]
pub struct Shutdown(Arc<State>);
impl Shutdown {
    pub fn new() -> io::Result<Self> {
        Ok(Self(Arc::new(State {
            requested: AtomicBool::new(false),
            wake: Wake::new()?,
        })))
    }
    /// Idempotent, nonblocking, allocation-free request. The eventfd stays
    /// readable, closing the race between checking the flag and entering poll.
    pub fn request(&self) {
        if !self.0.requested.swap(true, Ordering::Relaxed) {
            self.0.wake.notify();
        }
    }
    pub fn is_requested(&self) -> bool {
        self.0.requested.load(Ordering::Relaxed)
    }
    pub(crate) fn fd(&self) -> RawFd {
        self.0.wake.fd()
    }
}

impl std::os::fd::AsFd for Shutdown {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.0.wake.as_fd()
    }
}
