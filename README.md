# Snapshot Tool

Process metadata snapshots from DataCite and Crossref.

The Crossref Public Data File is available here: <https://www.crossref.org/learning/public-data-file/>

The DataCite Public Data File is available here: <https://support.datacite.org/docs/datacite-public-data-file>

It can:

- count the number of metadata records and generate other stats
- output list of DOIs
- combine multiple snapshot files into one single file

## Installation

Run `cargo install pardalotus_snapshot_tool`.

Or to build from source, run `cargo build --release`. The binary is available in the `./target/release` directory.

## Input

Supply the path to a directory or file with `--input`. This should contain all
snapshot files you're interested, including Crossref and/or DataCite files. It
will be scanned recursively, and files with unrecognised extensions will be
skipped.

The tool can accept files with extensions:

- `*.json.gz` (Crossref)
- `*.tgz` (DataCite)
- `*.jsonl.gz` - Output from this tool.

## Output

This tool can combine many files into one file. By supplying the `--out <filename>` you can combine all the data in the snapshot input
directory into one file.

## Functionality

### Show help

```
pardalotus_snapshot_tool --help
```

### Verbose

Add `--verbose` to any command for information on what's going on internally. Useful when reading large mysterious files.

### List files

```
pardalotus_snapshot_tool --input /path/to/snapshots --lit-input-files
```

This is useful for checking which snapshot files will be included.

### Count Records

```
pardalotus_snapshot_tool --input /path/to/snapshots --stats
```

Count how many metadata records are present across snapshots, as well as other stats.

## License

Copyright 2024 Joe Wass, Pardalotus Technology. This code is Apache 2.0 licensed, see the LICENSE.txt file.
