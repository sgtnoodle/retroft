//! Linked LZ4 blocks with fixed state and a caller-owned saved dictionary.
//! No source buffer remains borrowed after compression returns.

#[cfg(feature = "lz4")]
mod enabled {
    use anyhow::{ensure, Context, Result};
    use lz4::liblz4 as sys;
    use std::ptr::NonNull;

    const HISTORY_BYTES: usize = 64 * 1024;

    // Stable upstream APIs missing from the crate's bindings. The bundled C
    // implementation provides these symbols without experimental interfaces.
    extern "C" {
        fn LZ4_resetStream(stream: *mut sys::LZ4StreamEncode);
        fn LZ4_sizeofState() -> libc::c_int;
        fn LZ4_compress_fast_continue(
            stream: *mut sys::LZ4StreamEncode,
            source: *const u8,
            destination: *mut u8,
            source_size: libc::c_int,
            destination_size: libc::c_int,
            acceleration: libc::c_int,
        ) -> libc::c_int;
        fn LZ4_saveDict(
            stream: *mut sys::LZ4StreamEncode,
            dictionary: *mut u8,
            capacity: libc::c_int,
        ) -> libc::c_int;
    }

    pub struct Compressor {
        stream: NonNull<sys::LZ4StreamEncode>,
        history: Box<[u8]>,
        reset_bytes: usize,
        bytes_since_reset: usize,
        max_input: usize,
    }
    impl Compressor {
        pub fn new(reset_bytes: usize, max_input: usize) -> Result<Self> {
            ensure!(reset_bytes > 0, "compression reset size must be positive");
            ensure!(
                max_input > 0 && max_input <= retroft_format::MAX_PAYLOAD,
                "invalid LZ4 input bound"
            );
            let mut history = vec![0; HISTORY_BYTES].into_boxed_slice();
            for i in (0..HISTORY_BYTES).step_by(4096) {
                // Commit history pages during initialization.
                unsafe {
                    history.as_mut_ptr().add(i).write_volatile(0);
                }
            }
            let stream = NonNull::new(unsafe { sys::LZ4_createStream() })
                .context("initialize LZ4 stream")?;
            Ok(Self {
                stream,
                history,
                reset_bytes,
                bytes_since_reset: 0,
                max_input,
            })
        }
        /// Prepare a fresh history segment now; the next block carries its marker.
        pub fn reset(&mut self) {
            unsafe {
                LZ4_resetStream(self.stream.as_ptr());
            }
            self.bytes_since_reset = 0;
        }
        pub fn workspace_bytes(&self) -> usize {
            HISTORY_BYTES + unsafe { LZ4_sizeofState() as usize }
        }
        /// Returns encoded length and whether history was reset before this block.
        /// Call for raw-fallback blocks too: subsequent dictionary offsets depend
        /// on every trace block's input, whether stored raw or compressed.
        pub fn compress(&mut self, input: &[u8], output: &mut [u8]) -> Result<(usize, bool)> {
            ensure!(
                !input.is_empty() && input.len() <= self.max_input,
                "LZ4 input exceeds configured bound"
            );
            let capacity = super::compress_bound(input.len());
            ensure!(
                output.len() >= capacity,
                "LZ4 output below compression bound"
            );
            if self.bytes_since_reset != 0
                && input.len() > self.reset_bytes.saturating_sub(self.bytes_since_reset)
            {
                self.reset();
            }
            // Initialization/capture cleanup already reset the C stream.
            let reset = self.bytes_since_reset == 0;
            // SAFETY: exclusive stream; disjoint input/output slices are live for
            // this synchronous call. Bound-sized destination guarantees capacity.
            let count = unsafe {
                LZ4_compress_fast_continue(
                    self.stream.as_ptr(),
                    input.as_ptr(),
                    output.as_mut_ptr(),
                    input.len() as libc::c_int,
                    capacity as libc::c_int,
                    1,
                )
            };
            if count <= 0 {
                self.reset(); // A failed C stream must reset before another call.
                anyhow::bail!("LZ4 compression failed");
            }
            // The C stream would otherwise retain input's address. saveDict moves
            // its suffix into stable owned storage before the caller reuses input.
            // For noncontiguous small blocks, this retains the previous block's
            // suffix, not older blocks; the decoder may keep a longer history.
            let saved = unsafe {
                LZ4_saveDict(
                    self.stream.as_ptr(),
                    self.history.as_mut_ptr(),
                    HISTORY_BYTES as libc::c_int,
                )
            };
            ensure!(
                saved >= 0 && saved as usize <= HISTORY_BYTES,
                "LZ4 dictionary save failed"
            );
            self.bytes_since_reset = self.bytes_since_reset.saturating_add(input.len());
            Ok((count as usize, reset))
        }
    }
    impl Drop for Compressor {
        fn drop(&mut self) {
            // C state is released before Rust drops its referenced dictionary.
            unsafe {
                sys::LZ4_freeStream(self.stream.as_ptr());
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        extern "C" {
            fn LZ4_decompress_safe_usingDict(
                source: *const u8,
                destination: *mut u8,
                source_size: libc::c_int,
                destination_size: libc::c_int,
                dictionary: *const u8,
                dictionary_size: libc::c_int,
            ) -> libc::c_int;
        }
        fn random_bytes(len: usize) -> Vec<u8> {
            let mut x = 123u32;
            (0..len)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 17;
                    x ^= x << 5;
                    x as u8
                })
                .collect()
        }
        #[test]
        fn linked_blocks_survive_source_reuse_and_raw_fallback() -> Result<()> {
            let mut compressor = Compressor::new(1024 * 1024, 65536)?;
            let workspace = compressor.workspace_bytes();
            let original = random_bytes(32768);
            let mut source = original.clone();
            let mut packed = vec![0; super::super::compress_bound(source.len())];
            let mut dictionary = Vec::new();
            for iteration in 0..4 {
                source.copy_from_slice(&original);
                let (count, reset) = compressor.compress(&source, &mut packed)?;
                assert_eq!(reset, iteration == 0);
                let decoded = if iteration == 0 {
                    assert!(count >= source.len());
                    source.clone() // Simulate daemon raw fallback.
                } else {
                    assert!(count < source.len() / 10);
                    let mut decoded = vec![0; source.len()];
                    let count = unsafe {
                        LZ4_decompress_safe_usingDict(
                            packed.as_ptr(),
                            decoded.as_mut_ptr(),
                            count as libc::c_int,
                            decoded.len() as libc::c_int,
                            dictionary.as_ptr(),
                            dictionary.len() as libc::c_int,
                        )
                    };
                    assert_eq!(count as usize, decoded.len());
                    decoded
                };
                assert_eq!(decoded, original);
                dictionary.extend_from_slice(&decoded);
                if dictionary.len() > HISTORY_BYTES {
                    dictionary.drain(..dictionary.len() - HISTORY_BYTES);
                }
                source.fill(0); // The saved history must not borrow this buffer.
                assert_eq!(compressor.workspace_bytes(), workspace);
            }
            Ok(())
        }
        #[test]
        fn varying_block_sizes_decode_with_full_rolling_history() -> Result<()> {
            let mut compressor = Compressor::new(100_000, 65536)?;
            let original = random_bytes(65536);
            let mut output = vec![0; super::super::compress_bound(65536)];
            let mut dictionary = Vec::new();
            for size in [1, 4096, 1000, 32768, 4096, 65536, 2, 16384] {
                let mut source = original[..size].to_vec();
                let (count, reset) = compressor.compress(&source, &mut output)?;
                if reset {
                    dictionary.clear();
                }
                let mut decoded = vec![0; size];
                let decoded_size = unsafe {
                    LZ4_decompress_safe_usingDict(
                        output.as_ptr(),
                        decoded.as_mut_ptr(),
                        count as libc::c_int,
                        size as libc::c_int,
                        dictionary.as_ptr(),
                        dictionary.len() as libc::c_int,
                    )
                };
                assert_eq!(decoded_size as usize, size);
                assert_eq!(decoded, source);
                dictionary.extend_from_slice(&decoded);
                if dictionary.len() > HISTORY_BYTES {
                    dictionary.drain(..dictionary.len() - HISTORY_BYTES);
                }
                source.fill(0);
            }
            Ok(())
        }
        #[test]
        fn reset_cadence_and_oversized_blocks_are_bounded() -> Result<()> {
            let mut compressor = Compressor::new(100, 200)?;
            let mut output = vec![0; super::super::compress_bound(200)];
            for (size, expected_reset) in [
                (60, true),
                (40, false),
                (1, true),
                (200, true),
                (1, true),
                (99, false),
                (1, true),
            ] {
                assert_eq!(
                    compressor.compress(&vec![42; size], &mut output)?.1,
                    expected_reset
                );
            }
            compressor.reset();
            assert!(compressor.compress(&[42; 10], &mut output)?.1);
            assert!(compressor.compress(&[42; 201], &mut output).is_err());
            assert!(compressor.compress(&[42; 10], &mut [0; 1]).is_err());
            Ok(())
        }
    }
}

#[cfg(feature = "lz4")]
pub use enabled::Compressor;

pub fn compress_bound(size: usize) -> usize {
    #[cfg(feature = "lz4")]
    {
        if size > retroft_format::MAX_PAYLOAD {
            return usize::MAX;
        }
        unsafe { lz4::liblz4::LZ4_compressBound(size as libc::c_int) as usize }
    }
    #[cfg(not(feature = "lz4"))]
    {
        size
    }
}

#[cfg(not(feature = "lz4"))]
pub struct Compressor;
#[cfg(not(feature = "lz4"))]
impl Compressor {
    pub fn new(_reset_bytes: usize, _max_input: usize) -> anyhow::Result<Self> {
        anyhow::bail!("recorder was built without LZ4 support")
    }
    pub fn reset(&mut self) {}
    pub fn workspace_bytes(&self) -> usize {
        0
    }
    pub fn compress(&mut self, _input: &[u8], _output: &mut [u8]) -> anyhow::Result<(usize, bool)> {
        anyhow::bail!("recorder was built without LZ4 support")
    }
}
