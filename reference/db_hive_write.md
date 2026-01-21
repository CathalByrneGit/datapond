# Publish / Append / Ignore / Replace Partitions in the Hive Lake

Publish / Append / Ignore / Replace Partitions in the Hive Lake

## Usage

``` r
db_hive_write(
  data,
  section,
  dataset,
  partition_by = NULL,
  mode = c("overwrite", "append", "ignore", "replace_partitions"),
  compression = NULL,
  filename_pattern = "data_{uuid}"
)
```

## Arguments

- data:

  A data.frame / tibble

- section:

  Your section name

- dataset:

  The name of the dataset

- partition_by:

  Character vector of column names to partition by (e.g.
  c("year","month"))

- mode:

  One of:

  - "overwrite": replace target files

  - "append": add new files (requires unique filenames)

  - "ignore": write only if target path does not exist (best-effort;
    still race-prone)

  - "replace_partitions": delete only affected partition folders, then
    append fresh files (requires partition_by)

- compression:

  Parquet compression codec (NULL means DuckDB default). Options:
  "zstd", "snappy", "gzip", "brotli", "lz4", "lz4_raw", "uncompressed"

- filename_pattern:

  Used in append-like modes (default `"data_\{uuid\}"`)

## Value

Invisibly returns the output path

## Examples

``` r
if (FALSE) { # \dontrun{
# Basic overwrite
db_hive_write(my_data, "Trade", "Imports")

# Partitioned write
db_hive_write(my_data, "Trade", "Imports", partition_by = c("year", "month"))

# Append mode
db_hive_write(my_data, "Trade", "Imports", mode = "append")

# Replace only touched partitions
db_hive_write(my_data, "Trade", "Imports", 
              partition_by = c("year", "month"), 
              mode = "replace_partitions")
} # }
```
