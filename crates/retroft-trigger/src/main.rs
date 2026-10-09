use anyhow::{ensure, Context, Result};
use clap::Parser;
use std::{path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(version, about = "Trigger a Retroft capture")]
struct Cli {
    #[arg(long, default_value = "/run/retroft.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "manual trigger")]
    description: String,
    /// Human source label, e.g. a component or service name (not authenticated).
    #[arg(long, default_value = "retroft-trigger")]
    source: String,
    /// Absolute CLOCK_MONOTONIC nanoseconds; decimal integer.
    #[arg(long, conflicts_with = "post")]
    end_timestamp_ns: Option<u64>,
    /// Ask to capture this far into the future, e.g. 5s, 100ms, or 0.5s.
    #[arg(long, value_parser = positive_duration, allow_hyphen_values = true, conflicts_with = "end_timestamp_ns")]
    post: Option<Duration>,
}

fn positive_duration(value: &str) -> std::result::Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be at least one nanosecond".into());
    }
    u64::try_from(duration.as_nanos()).map_err(|_| "duration exceeds nanosecond range")?;
    Ok(duration)
}

fn endpoint(cli: &Cli, now_ns: u64) -> Result<Option<u64>> {
    let interval = cli
        .post
        .map(|duration| u64::try_from(duration.as_nanos()))
        .transpose()?;
    if let Some(interval) = interval {
        ensure!(interval > 0, "post duration must be positive");
        Ok(Some(
            now_ns.checked_add(interval).context("endpoint overflow")?,
        ))
    } else {
        Ok(cli.end_timestamp_ns)
    }
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    let endpoint = endpoint(&cli, retroft_trigger::monotonic_ns()?)?;
    let mut client = retroft_trigger::NonblockingClient::new(&cli.socket, &cli.source)?;
    let request = client.prepare(&cli.description, endpoint)?;
    client.try_send(&request)?;
    println!("trigger queued");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_units_compute_exact_endpoints() {
        let default = Cli::try_parse_from(["trigger"]).unwrap();
        assert!(default.post.is_none());
        assert_eq!(default.source, "retroft-trigger");
        assert_eq!(endpoint(&default, 100).unwrap(), None);
        for (value, interval) in [
            ("5s", 5_000_000_000),
            ("100ms", 100_000_000),
            ("0.5s", 500_000_000),
            ("500000000ns", 500_000_000),
            ("500ns", 500),
            ("1s 250ms", 1_250_000_000),
        ] {
            let cli = Cli::try_parse_from(["trigger", "--post", value]).unwrap();
            assert_eq!(endpoint(&cli, 100).unwrap(), Some(100 + interval));
        }
        let absolute = Cli::try_parse_from(["trigger", "--end-timestamp-ns", "123"]).unwrap();
        assert_eq!(endpoint(&absolute, u64::MAX).unwrap(), Some(123));
        assert!(
            Cli::try_parse_from(["trigger", "--post", "5s", "--end-timestamp-ns", "123"]).is_err()
        );
    }

    #[test]
    fn duration_validation_and_endpoint_overflow() {
        for value in [
            "NaN",
            "inf",
            "-1s",
            "0s",
            "0ns",
            "0.1ns",
            "1e20s",
            "18446744073709551616ns",
            "1",
            "invalid",
        ] {
            assert!(
                Cli::try_parse_from(["trigger", "--post", value]).is_err(),
                "accepted {value}"
            );
        }
        for flag in ["--post-ms", "--post-secs", "--post-ns"] {
            assert!(Cli::try_parse_from(["trigger", flag, "1"]).is_err());
        }
        let last = Cli::try_parse_from(["trigger", "--post", "5ns"]).unwrap();
        assert_eq!(endpoint(&last, u64::MAX - 5).unwrap(), Some(u64::MAX));
        assert!(endpoint(&last, u64::MAX - 4).is_err());
    }
}
