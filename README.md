# Snapshot Tool

Process metadata snapshots from DataCite and Crossref.

The Crossref Public Data File is available here: <https://www.crossref.org/learning/public-data-file/>

The DataCite Public Data File is available here: <https://support.datacite.org/docs/datacite-public-data-file>

It can:
 - count the number of metadata records
 - combine multiple snapshot files into one single file

## Installation

Run `cargo build --release`. The binary is available in the `./target/release` directory.

## Input

Supply the path to a directory with `--input-dir`. This should contain all
snapshot files you're interested, including Crossref and/or DataCite files. It
will be scanned recursively, and files with unrecognised extensions will be
skipped.

The tool can accept files with extensions:

 - `*.json.gz` (Crossref)
 - `*.tgz` (DataCite)
 - `*.jsonl.gz` - Output from this tool.

## Output

This tool can combine many files into one file. By supplying the `--out-format ".jsonl.gz" --out <filename>` you can combine all the data in the snapshot input directory into one file.

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

Crossref zipped JSON files are large chunks of API pages. They are currently buffered into memory and parsed. The initial parsing from Crossref format is therefore quite memory-intensive.

All functions will work from all input formats, but converting to `.jsonl.gz` first will give a speedup for further operations.

DataCite snapshot:
 - retrieved 2024
 - typical memory use is around 80MB
 - read 52,863,283 records
 - duration to parse and count all records 36.24 minutes (24,311 records per second)

Crossref snapshot:
 - retrieved 2024
 - typical memory use is around 300 MB
 - read 158,004,152 records
 - duration to count all records is 346 minutes (7,610 records per second)

Combined snapshot in `.jsonl.gz` format:
 - typical memory use is XX MB
  - duration to count all records is XX minutes
 - read XX records

Benchmarks on Intel Core i7-7700 4x Sky Lake with 64 GB RAM. 7200 RPM HDD.

## License

Copyright 2024 Joe Wass, Pardalotus Technology. This code is Apache 2.0 licensed, see the LICENSE.txt file.
