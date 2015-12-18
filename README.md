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
- **default_timezone**: Time zone of timestamp columns. This can be overwritten for each column using column_options
- **default_timestamp_format**: Format of timestamp columns. This can be overwritten for each column using column_options
- **column_options**: Specify timezone and timestamp format for each column. Format of this option is the same as the official csv formatter. See [document](
http://www.embulk.org/docs/built-in.html#csv-formatter-plugin).

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
