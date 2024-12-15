use structopt::StructOpt;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    println!("Pardalotus Snapshot Tool");

    let opt = Options::from_args();

    if opt.version {
        println!("Version {}", VERSION);
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "pardalotus_snapshot_tool", about = "Pardalotus Snapshot Tool")]
struct Options {
    /// Load tasks from directory at path
    #[structopt(long, help("Show version"))]
    version: bool,
}
