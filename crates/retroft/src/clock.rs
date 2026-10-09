//! Clock correlation sampled in userspace. Suspend boundaries between samples
//! are approximate; repeated suspend cycles between observations are inseparable.
use anyhow::{Context, Result};
use serde::Serialize;
use std::time::{Duration, Instant};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
// Ignore millisecond-scale bracket noise when detecting a suspend offset jump.
const RESUME_TOLERANCE_NS: u64 = 1_000_000;
// Room for one schema-v1 JSON snapshot, including full-width decimal integers.
pub const SNAPSHOT_RESERVE: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockSample {
    pub monotonic_ns: u64,
    pub boottime_ns: u64,
    pub realtime_ns: u64,
    /// Full width of the monotonic bracket, a conservative sampling bound.
    pub uncertainty_ns: u64,
}
impl ClockSample {
    fn offset(self) -> i128 {
        // BOOTTIME includes suspended time; MONOTONIC does not.
        i128::from(self.boottime_ns) - i128::from(self.monotonic_ns)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    FileStart,
    BeforeResume,
    Resume,
}

#[derive(Clone, Copy, Debug)]
pub struct ClockSnapshot {
    pub sample: ClockSample,
    pub reason: Reason,
}
fn decimal<S: serde::Serializer>(
    value: &u64,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    // JSON strings preserve nanoseconds beyond a double's exact integer range.
    serializer.collect_str(value)
}
impl ClockSnapshot {
    pub fn encode(self, output: &mut [u8]) -> Result<usize> {
        #[derive(Serialize)]
        struct Payload {
            schema_version: u32,
            #[serde(serialize_with = "decimal")]
            monotonic_ns: u64,
            #[serde(serialize_with = "decimal")]
            boottime_ns: u64,
            #[serde(serialize_with = "decimal")]
            realtime_ns: u64,
            #[serde(serialize_with = "decimal")]
            uncertainty_ns: u64,
            reason: Reason,
        }
        let mut writer = std::io::Cursor::new(output);
        serde_json::to_writer(
            &mut writer,
            &Payload {
                schema_version: 1,
                monotonic_ns: self.sample.monotonic_ns,
                boottime_ns: self.sample.boottime_ns,
                realtime_ns: self.sample.realtime_ns,
                uncertainty_ns: self.sample.uncertainty_ns,
                reason: self.reason,
            },
        )?;
        Ok(writer.position() as usize)
    }
}

fn sample_with(mut read: impl FnMut(libc::clockid_t) -> Result<u64>) -> Result<ClockSample> {
    for _ in 0..3 {
        // Nested brackets correlate all clocks without assuming atomic reads.
        let mono_before = read(libc::CLOCK_MONOTONIC)?;
        let boot_before = read(libc::CLOCK_BOOTTIME)?;
        let realtime_ns = read(libc::CLOCK_REALTIME)?;
        let boot_after = read(libc::CLOCK_BOOTTIME)?;
        let mono_after = read(libc::CLOCK_MONOTONIC)?;
        let mono_elapsed = mono_after
            .checked_sub(mono_before)
            .context("monotonic sampling clock moved backwards")?;
        let boot_elapsed = boot_after
            .checked_sub(boot_before)
            .context("boottime sampling clock moved backwards")?;
        // MONOTONIC excludes suspend, so its bracket alone cannot reveal a
        // thread frozen between clock reads. Reject a mixed suspend epoch.
        if boot_elapsed > mono_elapsed.saturating_add(RESUME_TOLERANCE_NS) {
            continue;
        }
        return Ok(ClockSample {
            monotonic_ns: mono_before + mono_elapsed / 2,
            boottime_ns: boot_before + boot_elapsed / 2,
            realtime_ns,
            uncertainty_ns: mono_elapsed.max(boot_elapsed),
        });
    }
    anyhow::bail!("clock sampling repeatedly crossed suspend; no consistent clock snapshot")
}
/// Read a Linux clock as nanoseconds, checking the conversion for overflow.
pub fn clock_ns(clock: libc::clockid_t) -> Result<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime receives a valid writable timespec.
    if unsafe { libc::clock_gettime(clock, &mut value) } != 0 {
        return Err(std::io::Error::last_os_error()).context("clock_gettime");
    }
    u64::try_from(value.tv_sec)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000_000_000))
        .and_then(|nanos| {
            u64::try_from(value.tv_nsec)
                .ok()
                .and_then(|fraction| nanos.checked_add(fraction))
        })
        .context("clock nanoseconds overflow")
}

pub fn sample() -> Result<ClockSample> {
    sample_with(clock_ns)
}

pub struct ClockMonitor {
    // Last sufficiently precise sample, used as the pre-resume baseline.
    last: ClockSample,
    next_sample: Instant,
    // Retain only resume pairs so later files can correlate retained history.
    history: Vec<ClockSnapshot>,
}
impl ClockMonitor {
    pub fn new(initial: ClockSample, now: Instant) -> Self {
        Self {
            last: initial,
            next_sample: now + SAMPLE_INTERVAL,
            history: Vec::new(),
        }
    }
    pub(crate) fn next_sample(&self) -> Instant {
        self.next_sample
    }
    pub fn due(&self, now: Instant) -> bool {
        now >= self.next_sample
    }
    pub fn history(&self) -> &[ClockSnapshot] {
        &self.history
    }
    pub fn history_bytes(&self) -> usize {
        self.history.capacity() * std::mem::size_of::<ClockSnapshot>()
    }
    fn observe(&mut self, current: ClockSample, now: Instant) -> bool {
        self.next_sample = now + SAMPLE_INTERVAL;
        // A slow/preempted observation is still useful with its stated bound,
        // but must not replace the baseline and mask a later resume forever.
        if current.uncertainty_ns > RESUME_TOLERANCE_NS {
            return false;
        }
        let threshold = u128::from(RESUME_TOLERANCE_NS)
            + u128::from(self.last.uncertainty_ns)
            + u128::from(current.uncertainty_ns);
        let resumed = (current.offset() - self.last.offset()).unsigned_abs() > threshold;
        if resumed {
            self.history.push(ClockSnapshot {
                sample: self.last,
                reason: Reason::BeforeResume,
            });
            self.history.push(ClockSnapshot {
                sample: current,
                reason: Reason::Resume,
            });
        }
        self.last = current;
        resumed
    }
    /// Also used immediately before file creation so a trigger need not wait
    /// for the next periodic observation to discover a resume.
    pub fn refresh(&mut self, now: Instant) -> Result<ClockSample> {
        let current = sample()?;
        self.observe(current, now);
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::ensure;
    fn reading(mono: u64, offset: u64, uncertainty: u64) -> ClockSample {
        ClockSample {
            monotonic_ns: mono,
            boottime_ns: mono + offset,
            realtime_ns: mono + offset + 1_000_000_000,
            uncertainty_ns: uncertainty,
        }
    }
    #[test]
    fn bracketing_reads_and_decimal_schema() -> Result<()> {
        let expected = [
            libc::CLOCK_MONOTONIC,
            libc::CLOCK_BOOTTIME,
            libc::CLOCK_REALTIME,
            libc::CLOCK_BOOTTIME,
            libc::CLOCK_MONOTONIC,
        ];
        let values = [100, 1000, 9_007_199_254_740_999, 1008, 110];
        let mut index = 0;
        let sample = sample_with(|clock| {
            ensure!(clock == expected[index], "unexpected clock sampling order");
            let result = values[index];
            index += 1;
            Ok(result)
        })?;
        assert_eq!(sample.monotonic_ns, 105);
        assert_eq!(sample.boottime_ns, 1004);
        assert_eq!(sample.uncertainty_ns, 10);
        let mut bytes = [0u8; SNAPSHOT_RESERVE];
        let length = ClockSnapshot {
            sample,
            reason: Reason::FileStart,
        }
        .encode(&mut bytes)?;
        let json: serde_json::Value = serde_json::from_slice(&bytes[..length])?;
        assert_eq!(json["monotonic_ns"], "105");
        assert_eq!(json["realtime_ns"], "9007199254740999");
        assert_eq!(json["reason"], "file_start");
        let mut backwards = [110, 1000, 123, 1001, 100].into_iter();
        assert!(sample_with(|_| Ok(backwards.next().unwrap())).is_err());
        Ok(())
    }
    #[test]
    fn monitor_keeps_only_pairs_around_detected_resumes() {
        let now = Instant::now();
        let mut monitor = ClockMonitor::new(reading(100, 0, 100), now);
        assert!(!monitor.due(now));
        assert!(monitor.due(now + SAMPLE_INTERVAL));
        assert!(!monitor.observe(reading(200, 20, 100), now));
        assert!(monitor.history().is_empty());
        assert!(monitor.observe(reading(300, 5_000_000_000, 100), now));
        assert_eq!(monitor.history().len(), 2);
        assert_eq!(monitor.history()[0].sample.monotonic_ns, 200);
        assert_eq!(monitor.history()[0].reason, Reason::BeforeResume);
        assert_eq!(monitor.history()[1].reason, Reason::Resume);
        assert!(!monitor.observe(reading(400, 5_000_000_020, 100), now));
        assert_eq!(monitor.history().len(), 2);
        assert!(monitor.observe(reading(500, 9_000_000_000, 100), now));
        assert_eq!(monitor.history().len(), 4);
    }
    #[test]
    fn uncertainty_prevents_false_resume_detection() {
        let now = Instant::now();
        let mut monitor = ClockMonitor::new(reading(0, 10_000_000, 5_000_000), now);
        assert!(!monitor.observe(reading(100, 14_000_000, 0), now));
        assert!(monitor.history().is_empty());
    }
    #[test]
    fn sampling_retries_a_suspend_crossing_and_bounds_retries() -> Result<()> {
        let mut readings = [
            100,
            100,
            1000,
            5_000_000_100,
            110,
            200,
            5_000_000_200,
            5_000_001_200,
            5_000_000_210,
            220,
        ]
        .into_iter();
        let result = sample_with(|_| Ok(readings.next().unwrap()))?;
        assert_eq!(result.monotonic_ns, 210);
        assert_eq!(result.boottime_ns, 5_000_000_205);
        let mut index = 0;
        let crossing = [100, 100, 1000, 5_000_000_100, 110];
        assert!(sample_with(|_| {
            let value = crossing[index % 5];
            index += 1;
            Ok(value)
        })
        .is_err());
        assert_eq!(index, 15);
        Ok(())
    }
    #[test]
    fn uncertain_observation_cannot_hide_a_resume_from_later_good_sample() {
        let now = Instant::now();
        let mut monitor = ClockMonitor::new(reading(100, 0, 100), now);
        assert!(!monitor.observe(reading(200, 5_000_000_000, 6_000_000_000), now));
        assert!(monitor.history().is_empty());
        assert!(monitor.observe(reading(300, 5_000_000_000, 100), now));
        assert_eq!(monitor.history()[0].sample.monotonic_ns, 100);
    }
}
