use std::{fs::File, io::BufWriter, path::PathBuf, sync::mpsc::Receiver};

use flate2::{write::GzEncoder, Compression};
use serde_json::Value;

use std::io::Write;

pub(crate) fn write_chan_to_json_gz(
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
