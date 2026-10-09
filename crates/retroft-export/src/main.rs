use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    version,
    about = "Decode a Retroft capture into native Perfetto protobuf"
)]
struct Cli {
    /// Capture files in consecutive capture-index order. One file permits recovery.
    #[arg(required = true, num_args = 1..)]
    input: Vec<PathBuf>,
    /// Destination trace. Existing files are refused.
    #[arg(short, long)]
    output: PathBuf,
    /// Keep disconnected leading events/losses and disable Perfetto common-start hints.
    #[arg(long)]
    raw: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let inputs: Vec<_> = cli.input.iter().map(|path| path.as_path()).collect();
    let report = retroft_export::offline::export_captures_with_options(
        &inputs,
        &cli.output,
        retroft_export::offline::ExportOptions { raw: cli.raw },
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
