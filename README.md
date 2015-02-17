# Parquet output plugin for Embulk


## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **path_prefix**: A prefix of output path. This is hadoop Path URI, and you can also include `scheme` and `authority` within this parameter. (string, required)
- **file_ext**: An extension of output path. (string, default: .parquet)
- **sequence_format**: (string, default: .%03d)
- **block_size**: A block size of parquet file. (int, default: 134217728(128M))
- **page_size**: A page size of parquet file. (int, default: 1048576(1M))
- **compression_codec**: A compression codec. available: UNCOMPRESSED, SNAPPY, GZIP (string, default: UNCOMPRESSED)
- **timezone**: A timezone for timestamp format. (string, default: UTC)

## Example

```yaml
out:
  type: parquet
  path_prefix: file:///data/output
```

## Build

```
$ ./gradlew gem
```
