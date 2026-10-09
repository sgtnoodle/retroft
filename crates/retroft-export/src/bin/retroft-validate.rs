use anyhow::{ensure, Result};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "retroft-validate",
    version,
    about = "Validate Retroft capture framing, checksums, metadata, and completion"
)]
struct Cli {
    /// Recorder capture file to inspect.
    input: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let report = retroft_export::capture::validate(&cli.input)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    ensure!(
        report.completed,
        "capture incomplete (valid prefix remains recoverable)"
    );
    Ok(())
}
