mod metadata;
mod read;
mod write;

use std::{
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
    list_files: bool,

    #[structopt(
        long,
        parse(from_os_str),
        help("Input directory containing snapshot files.")
    )]
    input_dir: Option<PathBuf>,

    #[structopt(long, help("Return stats for the snapshot files. Including count of records, total and average size of JSON, total and average size of DOIs."))]
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

    if options.list_files {
        main_list_files(&options)?;
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

fn main_list_files(options: &Options) -> Result<(), anyhow::Error> {
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
    let mut total_json_size: usize = 0;
    let mut total_doi_size: usize = 0;
    for record in rx.iter() {
        count += 1;
        total_json_size += record.to_string().len();
        total_doi_size += get_doi_from_record(&record)
            .and_then(|x| Some(x.len()))
            .unwrap_or(0 as usize);
    }
    println!("Count: {count}");
    println!("Total JSON bytes: {total_json_size}");

    let average_json_size = (total_json_size as f32) / (count as f32);
    println!("Average JSON bytes: {average_json_size}");

    println!("Total DOI bytes: {total_doi_size}");

    let average_doi_size = (total_json_size as f32) / (count as f32);
    println!("Average DOI bytes: {average_doi_size}");

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
    if let Some(ref input_dir) = options.input_dir {
        let files = find_input_files(input_dir)?;
        Ok((input_dir.clone(), files))
    } else {
        Err(anyhow::format_err!("Please supply <input-files>"))
    }
}

/// Return list of relevant files from directory.
fn find_input_files(input_dir: &std::path::PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = vec![];

    fn r(input_dir: &std::path::PathBuf, paths: &mut Vec<PathBuf>) -> anyhow::Result<()> {
        for entry in fs::read_dir(input_dir)? {
            let path = entry?.path();
            if path.is_file() {
                if let Some(path_str) = path.to_str() {
                    // Crossref public data file torrent is many `.json.gz` files.
                    if path_str.ends_with(".json.gz") ||
                    // DataCite public data file is one `.tgz` file with many `.jsonl` entries.
                    path_str.ends_with(".tgz") ||
                    // Format generated by this tool.
                    path_str.ends_with(".jsonl.gz")
                    {
                        paths.push(path);
                    }
                }
            } else if path.is_dir() {
                r(&path, paths)?;
            }
        }

        Ok(())
    }

    r(input_dir, &mut paths)?;

    Ok(paths)
}
