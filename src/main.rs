use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::PathBuf,
    process::exit,
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
};

use serde_json::Value;
use structopt::StructOpt;

use flate2::{read::GzDecoder, write::GzEncoder, Compression};
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

    #[structopt(
        long,
        short = "o",
        help("Save to output file. Only .jsonl.gz currently supported.")
    )]
    output_file: Option<PathBuf>,
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
        let (_, paths) = expect_input_files(&options)?;

        for path in paths {
            if let Some(path_str) = path.to_str() {
                println!("{}", path_str)
            }
        }
    }

    if options.count_records {
        let verbose = options.verbose;
        let (_, paths) = expect_input_files(&options)?;

        let (tx, rx): (SyncSender<Value>, Receiver<Value>) = mpsc::sync_channel(10);

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

    if let Some(ref output_file) = options.output_file {
        let verbose = options.verbose;
        let (input_dir, paths) = expect_input_files(&options)?;

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
    }

    Ok(())
}

fn write_chan_to_json_gz(
    output_file: &PathBuf,
    rx: Receiver<Value>,
    verbose: bool,
) -> anyhow::Result<()> {
    let f = File::create(output_file)?;
    let encoder = GzEncoder::new(f, Compression::best());
    let mut writer = BufWriter::new(encoder);

    let mut count: usize = 0;
    for entry in rx.iter() {
        serde_json::to_writer(&mut writer, &entry)?;
        writer.write(b"\n")?;

        count += 1;
        if verbose && count % 10000 == 0 {
            eprintln!("Written {} entries to {:?}", count, output_file);
        }
    }

    Ok(())
}

/// Read all entries in all files to the channel. One entry per message.
fn read_paths_to_channel(
    paths: &[PathBuf],
    tx: SyncSender<Value>,
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
            } else if path_str.ends_with(".jsonl.gz") {
                read_jsonl_gz_to_channel(path, &tx, verbose)?;
            }
        }
    }

    Ok(())
}

/// Read gzipped jsonl (JSON Lines) from a file to a channel, one string per line.
/// This format is generated by this tool.
fn read_jsonl_gz_to_channel(
    path: &PathBuf,
    channel: &SyncSender<Value>,
    verbose: bool,
) -> anyhow::Result<()> {
    let f = File::open(path)?;

    let decoded = BufReader::new(GzDecoder::new(f));

    let mut count: usize = 0;

    for line in decoded.lines() {
        let parsed: Value = serde_json::from_str(&line?)?;

        channel.send(parsed)?;
        count += 1;
        if verbose && count % 10000 == 0 {
            eprintln!("Read {} lines", count);
        }
    }

    Ok(())
}

/// Read a gzipped JSON file.
/// This is expected to be a Crossref file.
fn read_json_gz_to_channel(
    path: &PathBuf,
    tx: &SyncSender<Value>,
    verbose: bool,
) -> anyhow::Result<()> {
    if verbose {
        eprintln!("Reading .json.gz {:?}", &path);
    }

    let f = File::open(path)?;

    let json = BufReader::new(GzDecoder::new(f));
    let deserialized: Value = serde_json::from_reader(json)?;

    // Crossref files have a top-level key "items" containing items in that snapshot.
    if let Some(items) = deserialized.get("items").map(|x| x.as_array()).flatten() {
        let mut count: usize = 0;
        for item in items {
            // We're splitting the document into parts, so need to make a copy of this subtree.
            tx.send(item.clone())?;

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
    channel: &SyncSender<Value>,
    verbose: bool,
) -> anyhow::Result<()> {
    let tar_gz = File::open(path)?;
    let tar = BufReader::new(GzDecoder::new(tar_gz));

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
    channel: &SyncSender<Value>,
    verbose: bool,
) -> anyhow::Result<()> {
    let reader = io::BufReader::new(reader);

    let mut count: usize = 0;

    for line in reader.lines() {
        let parsed: Value = serde_json::from_str(&line?)?;

        channel.send(parsed)?;
        count += 1;
        if verbose && count % 10000 == 0 {
            eprintln!("Read {} lines", count);
        }
    }

    Ok(())
}

/// Return the input directory and a list of input files recursively found there.
/// Error if no option supplied.
fn expect_input_files(options: &Options) -> anyhow::Result<(PathBuf, Vec<PathBuf>)> {
    if let Some(ref input_dir) = options.input_dir {
        let files = find_input_files(&input_dir)?;
        return Ok((input_dir.clone(), files));
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
