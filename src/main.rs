use std::{fs, path::PathBuf};

use structopt::StructOpt;

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
}

fn main() {
    let opt = Options::from_args();

    if opt.version {
        println!("Version {}", VERSION);
    }

    if opt.list_files {
        if let Some(input_dir) = opt.input_dir {
            let mut paths: Vec<(FileType, PathBuf)> = vec![];
            match find_input_files(input_dir, &mut paths) {
                Ok(()) => {
                    for (_, path) in paths {
                        if let Some(path_str) = path.to_str() {
                            println!("{}", path_str)
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error scanning files: {:?}", e);
                }
            }
        } else {
            eprintln!("Please supply <path-dir>.")
        }
    }
}

enum FileType {
    JsonGz,
    Tgz,
}

/// Return list of relevant files from directory.
pub(crate) fn find_input_files(
    input_dir: std::path::PathBuf,
    result: &mut Vec<(FileType, PathBuf)>,
) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(input_dir)? {
        let path = entry?.path();
        if path.is_file() {
            if let Some(path_str) = path.to_str() {
                if path_str.ends_with(".json.gz") {
                    result.push((FileType::JsonGz, path));
                } else if path_str.ends_with(".tgz") {
                    result.push((FileType::Tgz, path));
                }
            }
        } else if path.is_dir() {
            find_input_files(path, result)?;
        }
    }

    Ok(())
}
