//! Storage with bounded submission and completion queues. Every pointer
//! submitted to io_uring refers to a stable
//! allocation that remains immutable until its write (including retries) ends.
use anyhow::{bail, ensure, Context, Result};
use io_uring::{opcode, squeue, types, IoUring};
use std::{
    fs::File,
    io,
    os::fd::{AsRawFd, RawFd},
};

const FILE_HEADER_LEN: u64 = 16;
// Completion tokens otherwise identify staging-block indices.
const SYNC_TOKEN: u64 = u64::MAX;
const DIRECTORY_SYNC_TOKEN: u64 = u64::MAX - 1;
// Reserve two queue slots for sync requests within a 32768-entry ring.
pub(crate) const MAX_BLOCKS: usize = 32766;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Free,
    // Fully framed, immutable record waiting for the previous batch to sync.
    Ready {
        offset: u64,
        len: usize,
    },
    Writing {
        // This record's fixed file range; short retries advance within it.
        offset: u64,
        len: usize,
        done: usize,
    },
}
struct Block {
    bytes: Box<[u8]>,
    state: State,
}

pub struct Storage {
    // Explicitly taken/dropped before allocations; Drop also waits for completions.
    ring: Option<IoUring>,
    blocks: Vec<Block>,
    file: File,
    // End of accepted records, including the next batch waiting in staging.
    logical_end: u64,
    submitted_end: u64,
    // Prefix covered by the last successful data sync.
    durable_prefix: u64,
    writes_pending: usize,
    sync_pending: bool,
    directory_pending: Option<File>,
    // Frozen when sync is submitted; later completion makes it durable.
    sync_prefix: u64,
    sync_bytes: u64,
    advised_prefix: u64,
    page_size: u64,
    advice_warned: bool,
    advice_warning: Option<i32>,
    // A failed request permanently disables further capture submissions.
    failure: Option<String>,
}

impl Storage {
    pub fn new(file: File, block_size: usize, block_count: usize, sync_bytes: u64) -> Result<Self> {
        ensure!(
            block_size > 0 && block_size <= u32::MAX as usize,
            "invalid staging block size"
        );
        ensure!(
            (1..=MAX_BLOCKS).contains(&block_count),
            "staging block count must be 1..32766"
        );
        ensure!(sync_bytes > 0, "sync batch size must be positive");
        block_size
            .checked_mul(block_count)
            .context("staging memory size overflow")?;
        let ring = IoUring::new((block_count + 2).next_power_of_two() as u32)
            .context("create io_uring (kernel support and permissions are required)")?;
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            // Writes touch every allocation page at startup instead of faulting
            // on first trace acquisition. Box allocation addresses never move.
            let mut bytes = vec![0u8; block_size].into_boxed_slice();
            for offset in (0..block_size).step_by(4096) {
                unsafe {
                    bytes.as_mut_ptr().add(offset).write_volatile(0);
                }
            }
            blocks.push(Block {
                bytes,
                state: State::Free,
            });
        }
        Ok(Self {
            ring: Some(ring),
            blocks,
            file,
            logical_end: FILE_HEADER_LEN,
            submitted_end: FILE_HEADER_LEN,
            durable_prefix: FILE_HEADER_LEN,
            writes_pending: 0,
            sync_pending: false,
            directory_pending: None,
            sync_prefix: FILE_HEADER_LEN,
            sync_bytes,
            advised_prefix: 0,
            page_size: system_page_size()?,
            advice_warned: false,
            advice_warning: None,
            failure: None,
        })
    }

    fn healthy(&self) -> Result<()> {
        if let Some(error) = &self.failure {
            bail!("storage previously failed: {error}");
        }
        Ok(())
    }
    fn poison<T>(&mut self, error: impl std::fmt::Display) -> Result<T> {
        let error = error.to_string();
        self.failure = Some(error.clone());
        bail!("{error}")
    }
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
    pub fn free_count(&self) -> usize {
        self.blocks
            .iter()
            .filter(|b| b.state == State::Free)
            .count()
    }
    pub fn pool_bytes(&self) -> usize {
        self.blocks.iter().map(|b| b.bytes.len()).sum()
    }
    /// Reuse the same allocations/ring for a new capture. The caller writes
    /// its header and separately requests directory name durability.
    pub fn replace_file(&mut self, file: File) -> Result<()> {
        self.healthy()?;
        ensure!(self.idle(), "cannot replace a file with outstanding I/O");
        ensure!(
            self.logical_end == self.durable_prefix,
            "previous capture requires finish before file replacement"
        );
        self.file = file;
        self.logical_end = FILE_HEADER_LEN;
        self.submitted_end = FILE_HEADER_LEN;
        // Replacement headers were written by the caller, but are not assumed
        // to have been synced. The first successful file sync covers them.
        self.durable_prefix = 0;
        self.sync_prefix = 0;
        self.advised_prefix = 0;
        Ok(())
    }
    pub fn logical_end(&self) -> u64 {
        self.logical_end
    }
    pub fn durable_prefix(&self) -> u64 {
        self.durable_prefix
    }
    /// POLLIN on the ring descriptor signals completion entries to reap.
    /// A closed ring has no wait source; poll ignores the -1 descriptor.
    pub(crate) fn completion_fd(&self) -> RawFd {
        self.ring.as_ref().map_or(-1, AsRawFd::as_raw_fd)
    }
    pub fn free_index(&self) -> Option<usize> {
        if self.failure.is_some() {
            return None;
        }
        self.blocks.iter().position(|b| b.state == State::Free)
    }
    pub fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        assert!(self.failure.is_none(), "storage unavailable");
        let block = &mut self.blocks[index];
        assert_eq!(
            block.state,
            State::Free,
            "cannot modify an accepted staging block"
        );
        &mut block.bytes
    }

    fn push(&mut self, entry: squeue::Entry) -> Result<()> {
        let ring = self.ring.as_mut().context("io_uring already closed")?;
        // SAFETY: write pointers belong to blocks marked Writing; sync has no
        // userspace pointers. Submission queue capacity exceeds simultaneous requests.
        unsafe { ring.submission().push(&entry) }
            .map_err(|_| anyhow::anyhow!("io_uring submission queue full"))?;
        loop {
            match ring.submit() {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error).context("submit io_uring request"),
            }
        }
    }
    fn write_entry(&self, index: usize) -> squeue::Entry {
        let block = &self.blocks[index];
        let State::Writing { offset, len, done } = block.state else {
            unreachable!()
        };
        opcode::Write::new(
            types::Fd(self.file.as_raw_fd()),
            unsafe { block.bytes.as_ptr().add(done) },
            (len - done) as u32,
        )
        .offset(offset + done as u64)
        .build()
        .user_data(index as u64)
    }
    pub fn submit(&mut self, index: usize, len: usize) -> Result<()> {
        self.healthy()?;
        let block = self
            .blocks
            .get(index)
            .context("invalid staging block index")?;
        ensure!(
            block.state == State::Free && len > 0 && len <= block.bytes.len(),
            "invalid staging write"
        );
        let end = next_offset(self.logical_end, len)?;
        self.blocks[index].state = State::Ready {
            offset: self.logical_end,
            len,
        };
        self.logical_end = end;
        self.pump()
    }

    /// Keep only one unsynced batch in the kernel. Compression can fill Ready
    /// blocks during write completion and sync, without growing the page cache.
    fn pump(&mut self) -> Result<()> {
        self.healthy()?;
        if self.sync_pending {
            return Ok(());
        }
        while self.submitted_end.saturating_sub(self.durable_prefix) < self.sync_bytes {
            let Some(index) = self.blocks.iter().position(|block| {
                matches!(block.state, State::Ready { offset, .. } if offset == self.submitted_end)
            }) else { break };
            let State::Ready { offset, len } = self.blocks[index].state else {
                unreachable!()
            };
            self.blocks[index].state = State::Writing {
                offset,
                len,
                done: 0,
            };
            self.submitted_end = next_offset(offset, len)?;
            self.writes_pending += 1;
            if let Err(error) = self.push(self.write_entry(index)) {
                return self.poison(error);
            }
        }
        // A record may cross the byte limit; never split its framing across batches.
        if self.submitted_end.saturating_sub(self.durable_prefix) >= self.sync_bytes
            && self.writes_pending == 0
        {
            self.request_sync()?;
        }
        Ok(())
    }

    pub fn reap(&mut self) -> Result<()> {
        self.healthy()?;
        self.reap_inner(true)?;
        self.pump()
    }
    fn reap_inner(&mut self, retry_short: bool) -> Result<()> {
        // Pop one completion entry at a time to avoid hot-path allocations and to
        // release the ring borrow before submitting a short-write retry.
        loop {
            let next = self
                .ring
                .as_mut()
                .context("io_uring already closed")?
                .completion()
                .next()
                .map(|c| (c.user_data(), c.result()));
            let Some((token, result)) = next else { break };
            if token == DIRECTORY_SYNC_TOKEN {
                if self.directory_pending.take().is_none() {
                    return self.poison("unexpected directory fsync completion");
                }
                if result < 0 {
                    return self.poison(format!(
                        "directory fsync failed: {}",
                        io::Error::from_raw_os_error(-result)
                    ));
                }
                if result != 0 {
                    return self.poison("unexpected positive directory fsync result");
                }
                continue;
            }
            if token == SYNC_TOKEN {
                self.complete_sync(result)?;
                continue;
            }
            let index = usize::try_from(token).context("invalid write completion token")?;
            if self.complete_write(index, result, retry_short)? {
                if let Err(error) = self.push(self.write_entry(index)) {
                    return self.poison(error);
                }
            }
        }
        Ok(())
    }
    // Return true only when the same immutable block needs a short-write retry.
    fn complete_write(&mut self, index: usize, result: i32, retry_short: bool) -> Result<bool> {
        let Some(block) = self.blocks.get_mut(index) else {
            return self.poison("unknown write completion token");
        };
        let State::Writing { offset, len, done } = block.state else {
            return self.poison("duplicate write completion");
        };
        if result <= 0 || result as usize > len - done {
            block.state = State::Free;
            self.writes_pending -= 1;
            return self.poison(if result < 0 {
                format!(
                    "capture write failed: {}",
                    io::Error::from_raw_os_error(-result)
                )
            } else {
                "invalid/zero-length write completion".into()
            });
        }
        let done = advance_written(done, len, result as usize)?;
        if done == len || !retry_short {
            block.state = State::Free;
            self.writes_pending -= 1;
            Ok(false)
        } else {
            block.state = State::Writing { offset, len, done };
            Ok(true)
        }
    }

    fn complete_sync(&mut self, result: i32) -> Result<()> {
        if !self.sync_pending {
            return self.poison("unexpected fsync completion");
        }
        self.sync_pending = false;
        if result < 0 {
            return self.poison(format!(
                "fdatasync failed: {}",
                io::Error::from_raw_os_error(-result)
            ));
        }
        if result != 0 {
            return self.poison("unexpected positive fsync result");
        }
        self.durable_prefix = self.sync_prefix;
        self.release_durable_cache();
        Ok(())
    }

    fn release_durable_cache(&mut self) {
        // Leave a shared partial page cached until a later sync covers it fully.
        let end = self.durable_prefix / self.page_size * self.page_size;
        if end <= self.advised_prefix {
            return;
        }
        let error = unsafe {
            libc::posix_fadvise64(
                self.file.as_raw_fd(),
                self.advised_prefix as libc::off64_t,
                (end - self.advised_prefix) as libc::off64_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        if error == 0 {
            self.advised_prefix = end;
        } else if !self.advice_warned {
            self.advice_warning = Some(error);
            self.advice_warned = true;
        }
    }

    pub fn take_warning(&mut self) -> Option<String> {
        self.advice_warning.take().map(|error| format!("warning: cannot release synced capture pages from cache: {}; further warnings suppressed", io::Error::from_raw_os_error(error)))
    }

    pub fn idle(&self) -> bool {
        self.writes_pending == 0
            && !self.sync_pending
            && self.directory_pending.is_none()
            && !self
                .blocks
                .iter()
                .any(|block| matches!(block.state, State::Ready { .. }))
    }
    /// Directory name durability is independent of data-buffer ownership. This
    /// request may overlap data writes so naming a new file never stalls its
    /// first history copies. The duplicate keeps the file descriptor open until completion.
    pub fn request_directory_sync(&mut self, directory: &File) -> Result<()> {
        self.healthy()?;
        ensure!(
            self.directory_pending.is_none(),
            "directory sync already pending"
        );
        let owned = directory.try_clone().context("retain directory sync fd")?;
        let entry = opcode::Fsync::new(types::Fd(owned.as_raw_fd()))
            .build()
            .user_data(DIRECTORY_SYNC_TOKEN);
        self.directory_pending = Some(owned);
        if let Err(error) = self.push(entry) {
            return self.poison(error);
        }
        Ok(())
    }
    pub fn request_sync(&mut self) -> Result<()> {
        self.healthy()?;
        if self.sync_pending {
            return Ok(());
        }
        ensure!(
            self.writes_pending == 0,
            "sync requires all prior write completions"
        );
        self.sync_prefix = self.submitted_end;
        self.sync_pending = true;
        let entry = opcode::Fsync::new(types::Fd(self.file.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(SYNC_TOKEN);
        if let Err(error) = self.push(entry) {
            return self.poison(error);
        }
        Ok(())
    }
    fn wait_one(&mut self) -> Result<()> {
        loop {
            match self
                .ring
                .as_mut()
                .context("io_uring already closed")?
                .submit_and_wait(1)
            {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error).context("wait for io_uring completion"),
            }
        }
    }
    pub fn finish(&mut self) -> Result<()> {
        self.healthy()?;
        while !self.idle() {
            self.wait_one()?;
            self.reap()?;
        }
        // Truncation is issued only with no outstanding writes. Its metadata is
        // then included in the final asynchronous fdatasync request.
        if let Err(error) = self.file.set_len(self.logical_end) {
            return self.poison(error);
        }
        self.request_sync()?;
        while !self.idle() {
            self.wait_one()?;
            self.reap()?;
        }
        Ok(())
    }
}

fn system_page_size() -> Result<u64> {
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    ensure!(
        size > 0,
        "cannot determine system page size for cache advice"
    );
    Ok(size as u64)
}

fn advance_written(done: usize, len: usize, result: usize) -> Result<usize> {
    ensure!(
        done <= len && result > 0 && result <= len - done,
        "invalid short-write progress"
    );
    Ok(done + result)
}
fn next_offset(offset: u64, len: usize) -> Result<u64> {
    let end = offset
        .checked_add(len as u64)
        .context("capture offset overflow")?;
    ensure!(end <= i64::MAX as u64, "capture exceeds file offset limit");
    Ok(end)
}
impl Drop for Storage {
    fn drop(&mut self) {
        // Even after an I/O failure, other requests can still own user pointers.
        // Drain their completion entries without scheduling retries. If ring communication
        // itself fails, retain allocations: closing a ring may
        // defer kernel cancellation, and freeing/reusing memory is unsafe.
        for block in &mut self.blocks {
            if matches!(block.state, State::Ready { .. }) {
                block.state = State::Free;
            }
        }
        while !self.idle() {
            if self.wait_one().is_err() {
                break;
            }
            let _ = self.reap_inner(false);
        }
        drop(self.ring.take());
        if !self.idle() {
            for block in self.blocks.drain(..) {
                std::mem::forget(block.bytes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    // No kernel ring: deterministic tests control completion order explicitly.
    fn controlled_storage() -> Storage {
        Storage {
            ring: None,
            blocks: (0..3)
                .map(|_| Block {
                    bytes: vec![0; 64].into_boxed_slice(),
                    state: State::Free,
                })
                .collect(),
            file: tempfile::tempfile().unwrap(),
            logical_end: 48,
            submitted_end: 48,
            durable_prefix: 16,
            writes_pending: 0,
            sync_pending: true,
            directory_pending: None,
            sync_prefix: 48,
            sync_bytes: 32,
            advised_prefix: 0,
            page_size: 4096,
            advice_warned: false,
            advice_warning: None,
            failure: None,
        }
    }

    #[test]
    fn next_batch_can_stage_during_sync_without_becoming_durable() {
        let mut storage = controlled_storage();
        storage.buffer_mut(0)[..4].copy_from_slice(b"next");
        storage.submit(0, 4).unwrap();
        assert_eq!(storage.blocks[0].state, State::Ready { offset: 48, len: 4 });
        assert_eq!(storage.logical_end(), 52);
        assert_eq!(storage.submitted_end, 48);
        assert_eq!(storage.free_count(), 2);
        assert!(!storage.idle());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            storage.buffer_mut(0)[0] = b'x';
        }))
        .is_err());
        storage.complete_sync(0).unwrap();
        assert_eq!(storage.durable_prefix(), 48);
        assert_eq!(&storage.blocks[0].bytes[..4], b"next");
        assert_eq!(storage.blocks[0].state, State::Ready { offset: 48, len: 4 });
        assert!(!storage.idle());
        // Drop discards unsubmitted Ready bytes without waiting on a kernel ring.
    }

    #[test]
    fn failed_sync_preserves_previous_durability_and_stops_admission() {
        let mut storage = controlled_storage();
        storage.submit(0, 4).unwrap();
        assert!(storage.complete_sync(-libc::EIO).is_err());
        assert_eq!(storage.durable_prefix(), 16);
        assert_eq!(storage.blocks[0].state, State::Ready { offset: 48, len: 4 });
        assert!(storage.free_index().is_none());
        assert!(storage.submit(1, 4).is_err());
    }

    #[test]
    fn short_retry_retains_ownership_until_all_delayed_writes_complete() {
        let mut storage = controlled_storage();
        storage.sync_pending = false;
        storage.blocks[0].state = State::Writing {
            offset: 16,
            len: 12,
            done: 0,
        };
        storage.blocks[1].state = State::Writing {
            offset: 28,
            len: 20,
            done: 0,
        };
        storage.writes_pending = 2;
        assert!(storage.complete_write(0, 5, true).unwrap());
        assert_eq!(storage.writes_pending, 2);
        assert_eq!(
            storage.blocks[0].state,
            State::Writing {
                offset: 16,
                len: 12,
                done: 5
            }
        );
        assert!(storage.request_sync().is_err());
        assert!(!storage.complete_write(1, 20, true).unwrap());
        assert_eq!(storage.writes_pending, 1);
        assert!(storage.request_sync().is_err());
        assert!(!storage.complete_write(0, 7, true).unwrap());
        assert_eq!(storage.writes_pending, 0);
        assert_eq!(storage.durable_prefix(), 16);
        assert!(storage.idle());
    }

    #[test]
    fn cache_advice_leaves_shared_partial_page_until_next_sync() {
        let mut storage = controlled_storage();
        storage.sync_prefix = 4097;
        storage.complete_sync(0).unwrap();
        assert_eq!(storage.advised_prefix, 4096);
        storage.sync_pending = true;
        storage.sync_prefix = 8191;
        storage.complete_sync(0).unwrap();
        assert_eq!(storage.advised_prefix, 4096);
        storage.sync_pending = true;
        storage.sync_prefix = 8192;
        storage.complete_sync(0).unwrap();
        assert_eq!(storage.advised_prefix, 8192);
    }

    #[test]
    fn short_write_progress_rejects_zero_and_overflow() {
        assert_eq!(advance_written(0, 12, 5).unwrap(), 5);
        assert_eq!(advance_written(5, 12, 7).unwrap(), 12);
        assert!(advance_written(5, 12, 0).is_err());
        assert!(advance_written(5, 12, 8).is_err());
        assert!(advance_written(13, 12, 1).is_err());
    }
    #[test]
    fn offsets_reject_file_limits_and_overflow() {
        assert_eq!(
            next_offset(FILE_HEADER_LEN, 10).unwrap(),
            FILE_HEADER_LEN + 10
        );
        assert!(next_offset(i64::MAX as u64, 1).is_err());
        assert!(next_offset(u64::MAX, 1).is_err());
    }
    #[test]
    fn async_records_are_ordered_and_durable_if_supported() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[0u8; 16]).unwrap();
        file.sync_data().unwrap();
        let copy = file.try_clone().unwrap();
        let mut storage = match Storage::new(file, 4096, 2, 3) {
            Ok(storage) => storage,
            Err(error)
                if error.chain().any(|e| {
                    e.downcast_ref::<io::Error>().is_some_and(|e| {
                        matches!(
                            e.raw_os_error(),
                            Some(libc::EPERM | libc::EACCES | libc::ENOSYS)
                        )
                    })
                }) =>
            {
                eprintln!("SKIP real io_uring integration: {error:#}");
                return;
            }
            Err(error) => panic!("{error:#}"),
        };
        let block_address = storage.buffer_mut(0).as_ptr() as usize;
        let mut completion = libc::pollfd {
            fd: storage.completion_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // Writable ring capacity must not wake a completion-only wait.
        assert_eq!(unsafe { libc::poll(&mut completion, 1, 0) }, 0);
        storage.buffer_mut(0)[..3].copy_from_slice(b"abc");
        storage.submit(0, 3).unwrap();
        storage.buffer_mut(1)[..4].copy_from_slice(b"defg");
        storage.submit(1, 4).unwrap();
        // The first record reaches the byte threshold. The second is ready for
        // compression/write pipelining, but cannot enter this unsynced batch.
        assert_eq!(storage.submitted_end, 19);
        assert_eq!(storage.blocks[1].state, State::Ready { offset: 19, len: 4 });
        let directory = tempfile::tempdir().unwrap();
        let directory_file = File::open(directory.path()).unwrap();
        storage.request_directory_sync(&directory_file).unwrap();
        drop(directory_file); // Storage owns the duplicate until completion.
        assert_eq!(storage.logical_end(), 23);
        assert_eq!(unsafe { libc::poll(&mut completion, 1, 1000) }, 1);
        assert_ne!(completion.revents & libc::POLLIN, 0);
        assert!(storage.request_sync().is_err());
        storage.finish().unwrap();
        assert_eq!(storage.durable_prefix(), 23);
        assert_eq!(storage.free_count(), 2);
        let mut bytes = Vec::new();
        let mut copy = copy;
        use std::io::{Seek, SeekFrom};
        copy.seek(SeekFrom::Start(0)).unwrap();
        copy.read_to_end(&mut bytes).unwrap();
        assert_eq!(&bytes[16..], b"abcdefg");
        let mut next = tempfile::tempfile().unwrap();
        next.write_all(&[0u8; 16]).unwrap();
        next.sync_data().unwrap();
        let mut next_copy = next.try_clone().unwrap();
        storage.replace_file(next).unwrap();
        assert_eq!(storage.durable_prefix(), 0);
        assert_eq!(storage.buffer_mut(0).as_ptr() as usize, block_address);
        storage.buffer_mut(0)[..2].copy_from_slice(b"hi");
        storage.submit(0, 2).unwrap();
        storage.finish().unwrap();
        assert_eq!(storage.logical_end(), 18);
        assert_eq!(storage.durable_prefix(), 18);
        next_copy.seek(SeekFrom::Start(0)).unwrap();
        bytes.clear();
        next_copy.read_to_end(&mut bytes).unwrap();
        assert_eq!(&bytes[16..], b"hi");
    }
}
