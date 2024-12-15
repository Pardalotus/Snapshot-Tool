use std::{
    fs::{self, File},
    io::{self, BufRead, Read},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    thread,
};

use serde_json::Value;
use structopt::StructOpt;

use flate2::read::GzDecoder;
use tar::Archive;

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

    #[structopt(long, help("Count all metadata records in snapshot files."))]
    count_records: bool,

    #[structopt(long, short = "v", help("Send progress messages to STDERR."))]
    verbose: bool,
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
        let paths = expect_input_files(&options)?;

        for path in paths {
            if let Some(path_str) = path.to_str() {
                println!("{}", path_str)
            }
        }
    }

    if options.count_records {
        let verbose = options.verbose;
        let paths = expect_input_files(&options)?;

        let (tx, rx): (Sender<String>, Receiver<String>) = mpsc::channel();

        let read_thread = thread::spawn(move || {
            if let Err(err) = read_paths_to_channel(&paths, tx, verbose) {
                eprintln!("Failed read archives: {:?}", err);
            }
        });

        // No verbose messages here to avoid crashing thread's messages.
        let mut count: usize = 0;
        for _ in rx.iter() {
            count += 1;
        }
        println!("{count}");

        read_thread
            .join()
            .unwrap_or_else(|err| eprintln!("Failed to join reader thread: {:?}", err));
    }

    Ok(())
}

/// Read all entries in all files to the channel. One entry per message.
fn read_paths_to_channel(
    paths: &[PathBuf],
    tx: Sender<String>,
    verbose: bool,
) -> anyhow::Result<()> {
    for ref path in paths.iter() {
        // path::ends_with comparison for path doesn't work for sub-path-component chunks.
        // path::extension only takes the lats extension files so is unsuitbale for `.tar.gz`.
        if let Some(path_str) = path.to_str() {
            // Ignore other types.
            if path_str.ends_with(".tgz") {
                read_tgz_to_channel(path, &tx, verbose)?;
            } else if path_str.ends_with(".json.gz") {
                read_json_gz_to_channel(path, &tx, verbose)?;
            }
        }
    }

    Ok(())
}

/// Read a gzipped JSON file.
/// This is expected to be a Crossref file.
fn read_json_gz_to_channel(
    path: &PathBuf,
    tx: &Sender<String>,
    verbose: bool,
) -> anyhow::Result<()> {
    if verbose {
        eprintln!("Reading .json.gz {:?}", &path);
    }

    let f = File::open(path)?;
    let json = GzDecoder::new(f);
    let deserialized: Value = serde_json::from_reader(json)?;

    // Crossref files have a top-level key "items" containing items in that snapshot.
    if let Some(items) = deserialized.get("items").map(|x| x.as_array()).flatten() {
        let mut count: usize = 0;
        for item in items {
            tx.send(item.to_string())?;

            count += 1;
            if verbose && count % 10000 == 0 {
                eprintln!("From {:?} read {} lines", path, count);
            }
        }
    } else {
        eprint!("Didn't get recognised JSON format from {:?}", path);
    }

    if verbose {
        eprintln!("Finished reading .json.gz {:?}", &path);
    }

    Ok(())
}

/// Read all entries in all files in a gzipped tar file to a channel.
fn read_tgz_to_channel(
    path: &PathBuf,
    channel: &Sender<String>,
    verbose: bool,
) -> anyhow::Result<()> {
    let tar_gz = File::open(path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    if verbose {
        eprintln!("Read TGZ {:?}", path);
    }

    for entry in archive.entries()? {
        let mut ok_entry = entry?;
        let entry_path = ok_entry.path()?;

        if entry_path
            .file_name()
            .map(|x| x.to_str())
            .flatten()
            .map(|x| x.ends_with(".jsonl"))
            .unwrap_or(false)
        {
            if verbose {
                eprintln!("From TGZ {:?} read {:?}", path, entry_path);
            }

            read_jsonl_to_channel(&mut ok_entry, channel, verbose)?;
        }
    }

    if verbose {
        eprintln!("Finished reading TGZ {:?}", path);
    }

    Ok(())
}

/// Read a jsonl (JSON Lines) reader to a channel, one string per line.
/// These are expected to be found in DataCite snapshots.
fn read_jsonl_to_channel(
    reader: &mut dyn Read,
    channel: &Sender<String>,
    verbose: bool,
) -> anyhow::Result<()> {
    let reader = io::BufReader::new(reader);

    let mut count: usize = 0;

    for line in reader.lines() {
        channel.send(line?)?;
        count += 1;
        if verbose && count % 10000 == 0 {
            eprintln!("Read {} lines", count);
        }
    }

    Ok(())
}

/// Get a list of input files. Error if no option supplied.
fn expect_input_files(options: &Options) -> anyhow::Result<Vec<PathBuf>> {
    if let Some(ref input_dir) = options.input_dir {
        let files = find_input_files(&input_dir)?;
        return Ok(files);
    } else {
        return Err(anyhow::format_err!("Please supply <input-files>"));
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
                    path_str.ends_with(".tgz")
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
