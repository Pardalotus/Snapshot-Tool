# Snapshot Tool

Process metadata snapshots from DataCite and Crossref.

The Crossref Public Data File is available here: <https://www.crossref.org/learning/public-data-file/>

The DataCite Public Data File is available here: <https://support.datacite.org/docs/datacite-public-data-file>

## Input

Supply the path to a directory with `--input-dir`. This should contain all
snapshot files you're interested, including Crossref and/or DataCite files. It
will be scanned recursively, and files with unrecognised extensions will be
skipped.

The tool expects files with extensions:

 - `*.json.gz` (Crossref)
 - `*.tgz` (DataCite)

## Installation

Run `cargo build --release`. The binary is available in the `./target/release` directory.

## Functionality

### Show help
```
pardalotus_snapshot_tool --help
```

### Verbose

Add `--verbose` to any command for information on what's going on internally. Useful when reading large mysterious files.

### List files
```
pardalotus_snapshot_tool --input-dir /path/to/snapshots
```
This is useful for checking which snapshot files will be included.

### Count Records
```
pardalotus_snapshot_tool --input-dir /path/to/snapshots --count-records
```

Count how many metadata records are present across snapshots.

## Limitations

The Crossref zipped JSON files are currently buffered into memory, meaning ~20MB sized allocations. The initial parsing is therefore quite memory-intensive.

When reading a DataCite snapshot typical memory use is 75MB.

When reading the Crossref snapshot the typical memory use is 300 MB.

## License

Copyright 2024 Joe Wass, Pardalotus Technology. This code is Apache 2.0 licensed, see the LICENSE.txt file.
