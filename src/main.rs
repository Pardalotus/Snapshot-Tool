mod metadata;
mod read;
mod write;

use std::{
    collections::BTreeMap,
    fs::{self},
    path::PathBuf,
    process::exit,
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
};

use metadata::get_doi_from_record;
use read::read_paths_to_channel;
use serde_json::Value;
use structopt::StructOpt;

use write::write_chan_to_json_gz;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, StructOpt)]
#[structopt(name = "pardalotus_snapshot_tool", about = "Pardalotus Snapshot Tool")]
struct Options {
    #[structopt(long, help("Show version"))]
    version: bool,

    #[structopt(long, help("List all snapshot files found in the input directory."))]
    list_input_files: bool,

    #[structopt(
        long,
        parse(from_os_str),
        help("Input directory containing snapshot files.")
    )]
    input: Option<PathBuf>,

    #[structopt(long, help("Return stats for the snapshot files. Including count of records, total and mean size of JSON, total and mean size of DOIs."))]
    stats: bool,

    #[structopt(long, short = "v", help("Send progress messages to STDERR."))]
    verbose: bool,

    #[structopt(
        long,
        short = "o",
        help("Save to output file, combining all inputs. Only .jsonl.gz currently supported.")
    )]
    output_file: Option<PathBuf>,

    #[structopt(long, help("Print list of DOIs for all records to STDOUT."))]
    print_dois: bool,
}

fn main() {
    match main_r() {
        Ok(()) => std::process::exit(0),
        Err(err) => {
            eprintln!("Error: {:?}", err);
            std::process::exit(1);
        }
    }
}

fn main_r() -> anyhow::Result<()> {
    let options = Options::from_args();

    if options.version {
        println!("Version {}", VERSION);
    }

    if options.list_input_files {
        main_list_input_files(&options)?;
    }

    if options.stats {
        main_stats(&options)?;
    }

    if options.print_dois {
        main_print_dois(&options)?;
    }

    if let Some(ref output_file) = options.output_file {
        main_output_file(&options, output_file)?;
    }

    Ok(())
}

fn main_list_input_files(options: &Options) -> Result<(), anyhow::Error> {
    let (_, paths) = expect_input_files(options)?;
    for path in paths {
        if let Some(path_str) = path.to_str() {
            println!("{}", path_str)
        }
    }
    Ok(())
}

fn main_stats(options: &Options) -> Result<(), anyhow::Error> {
    let verbose = options.verbose;
    let (_, paths) = expect_input_files(options)?;
    let (tx, rx): (SyncSender<Value>, Receiver<Value>) = mpsc::sync_channel(10);
    let read_thread = thread::spawn(move || {
        if let Err(err) = read_paths_to_channel(&paths, tx, verbose) {
            eprintln!("Failed read archives: {:?}", err);
        }
    });
    let mut count: usize = 0;

    let mut total_json_chars: usize = 0;
    let mut total_doi_bytes: usize = 0;
    let mut total_doi_chars: usize = 0;
    let mut doi_chars_frequencies = BTreeMap::<usize, usize>::new();
    let mut doi_bytes_frequencies = BTreeMap::<usize, usize>::new();
    let mut json_chars_frequencies = BTreeMap::<usize, usize>::new();
    let mut max_doi_codepoint: char = '\0';

    for record in rx.iter() {
        count += 1;

        if verbose && count % 10000 == 0 {
            eprintln!("Read {} lines", count);
        }

        let json_chars = record.to_string().len();
        total_json_chars += json_chars;

        // Integer division to bucket into 1kb buckets.
        let json_chars_bucketed = (json_chars / 1024) * 1024;
        *json_chars_frequencies
            .entry(json_chars_bucketed)
            .or_insert(0) += 1;

        if let Some(doi) = get_doi_from_record(&record) {
            let doi_chars = doi.len();
            let doi_bytes = doi.as_bytes().len();

            if let Some(this_max_doi_codepoint) = doi.chars().max() {
                max_doi_codepoint = this_max_doi_codepoint.max(max_doi_codepoint);
            }

            total_doi_chars += doi_chars;
            *doi_chars_frequencies.entry(doi_chars).or_insert(0) += 1;

            total_doi_bytes += doi_bytes;
            *doi_bytes_frequencies.entry(doi_chars).or_insert(0) += 1;
        }
    }

    let mean_json_chars = (total_json_chars as f32) / (count as f32);
    let mean_doi_chars = (total_doi_chars as f32) / (count as f32);
    let mean_doi_bytes = (total_doi_bytes as f32) / (count as f32);

    let mode_doi_chars = doi_chars_frequencies
        .iter()
        .max_by_key(|&(_, count)| count)
        .map(|(value, _)| value)
        .unwrap_or(&0);

    let mode_doi_bytes = doi_bytes_frequencies
        .iter()
        .max_by_key(|&(_, count)| count)
        .map(|(value, _)| value)
        .unwrap_or(&0);

    let mode_json_chars = json_chars_frequencies
        .iter()
        .max_by_key(|&(_, count)| count)
        .map(|(value, _)| value)
        .unwrap_or(&0);

    println!("Record count: {count}");
    println!("");
    println!("JSON:");
    println!("Total JSON chars: {total_json_chars}");
    println!("Mean JSON chars: {mean_json_chars}");
    println!("Modal JSON chars: {mode_json_chars}");

    println!("");
    println!("DOIs:");
    println!("Total DOI chars: {total_doi_chars}");
    println!("Mean DOI chars: {mean_doi_chars}");
    println!("Modal DOI chars: {mode_doi_chars}");

    println!("");

    println!("Total DOI bytes: {total_doi_bytes}");
    println!("Mean DOI bytes: {mean_doi_bytes}");
    println!("Modal DOI bytes: {mode_doi_bytes}");

    println!(
        "Max Unicode code point: {} : {}",
        max_doi_codepoint, max_doi_codepoint as u32
    );

    println!("");
    println!("Frequencies:");
    println!("JSON chars frequencies (bins of 1KiB):");

    for (length, frequency) in json_chars_frequencies.into_iter() {
        println!("{length},{frequency}");
    }
    println!("");
    println!("");

    println!("DOI chars frequencies:");

    for (length, frequency) in doi_chars_frequencies.into_iter() {
        println!("{length},{frequency}");
    }

    read_thread
        .join()
        .unwrap_or_else(|err| eprintln!("Failed to join reader thread: {:?}", err));
    Ok(())
}

fn main_print_dois(options: &Options) -> Result<(), anyhow::Error> {
    let verbose = options.verbose;
    let (_, paths) = expect_input_files(options)?;
    let (tx, rx): (SyncSender<Value>, Receiver<Value>) = mpsc::sync_channel(10);
    let read_thread = thread::spawn(move || {
        if let Err(err) = read_paths_to_channel(&paths, tx, verbose) {
            eprintln!("Failed read archives: {:?}", err);
        }
    });
    for rec in rx.iter() {
        if let Some(doi) = get_doi_from_record(&rec) {
            println!("{}", doi);
        }
    }
    read_thread
        .join()
        .unwrap_or_else(|err| eprintln!("Failed to join reader thread: {:?}", err));
    Ok(())
}

fn main_output_file(options: &Options, output_file: &PathBuf) -> Result<(), anyhow::Error> {
    let verbose = options.verbose;
    let (input_dir, paths) = expect_input_files(options)?;
    if output_file.starts_with(&input_dir) {
        eprint!(
            "Output file {:?} can't be in the input directory {:?}",
            output_file, input_dir
        );
        exit(1);
    }
    let (tx, rx): (SyncSender<Value>, Receiver<Value>) = mpsc::sync_channel(10);
    let read_thread = thread::spawn(move || {
        if let Err(err) = read_paths_to_channel(&paths, tx, verbose) {
            eprintln!("Failed read archives: {:?}", err);
        }
    });
    write_chan_to_json_gz(output_file, rx, verbose)?;
    read_thread
        .join()
        .unwrap_or_else(|err| eprintln!("Failed to join reader thread: {:?}", err));
    Ok(())
}

/// Return the input directory and a list of input files recursively found there.
/// Error if no option supplied.
fn expect_input_files(options: &Options) -> anyhow::Result<(PathBuf, Vec<PathBuf>)> {
    if let Some(ref input_dir) = options.input {
        let files = find_input_files(input_dir)?;
        Ok((input_dir.clone(), files))
    } else {
        Err(anyhow::format_err!("Please supply <input>"))
    }
}

/// Return list of relevant files from path. If it's a directory, recurse.
fn find_input_files(input_path: &std::path::PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = vec![];

    fn r(path: &std::path::PathBuf, paths: &mut Vec<PathBuf>) -> anyhow::Result<()> {
        if path.is_file() {
            if let Some(path_str) = path.to_str() {
                // Crossref public data file torrent is many `.json.gz` files.
                if path_str.ends_with(".json.gz") ||
                    // DataCite public data file is one `.tgz` file with many `.jsonl` entries.
                    path_str.ends_with(".tgz") ||
                    // Format generated by this tool.
                    path_str.ends_with(".jsonl.gz")
                {
                    paths.push(path.clone());
                }
            }
            Ok(())
        } else if path.is_dir() {
            for entry in fs::read_dir(path)? {
                let path = entry?.path();
                r(&path, paths)?
            }

            Ok(())
        } else {
            Ok(())
        }
    }

    r(input_path, &mut paths)?;

    Ok(paths)
}
