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
        let paths = get_input_files(options)?;

        for (_, path) in paths {
            if let Some(path_str) = path.to_str() {
                println!("{}", path_str)
            }
        }
    }

    Ok(())
}

enum FileType {
    JsonGz,
    Tgz,
}

fn get_input_files(options: Options) -> anyhow::Result<Vec<(FileType, PathBuf)>> {
    if let Some(input_dir) = options.input_dir {
        let files = find_input_files(input_dir)?;
        return Ok(files);
    } else {
        return Err(anyhow::format_err!("Please supply <input-files>"));
    }
}

/// Return list of relevant files from directory.
fn find_input_files(input_dir: std::path::PathBuf) -> anyhow::Result<Vec<(FileType, PathBuf)>> {
    let mut paths: Vec<(FileType, PathBuf)> = vec![];

    fn r(
        input_dir: std::path::PathBuf,
        paths: &mut Vec<(FileType, PathBuf)>,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(input_dir)? {
            let path = entry?.path();
            if path.is_file() {
                if let Some(path_str) = path.to_str() {
                    if path_str.ends_with(".json.gz") {
                        paths.push((FileType::JsonGz, path));
                    } else if path_str.ends_with(".tgz") {
                        paths.push((FileType::Tgz, path));
                    }
                }
            } else if path.is_dir() {
                r(path, paths)?;
            }
        }

        Ok(())
    }

    r(input_dir, &mut paths)?;

    Ok(paths)
}
