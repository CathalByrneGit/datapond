# Compact small files in a DuckLake table

Merges small Parquet files into larger ones to improve query
performance. When data is written in small batches, DuckLake creates
many small files which slows down reads. Compaction consolidates these
files.

**When to compact:**

- After many small inserts (e.g., streaming data, row-by-row imports)

- When
  [`db_file_stats()`](https://cathalbyrnegit.github.io/datapond/reference/db_file_stats.md)
  shows high file counts with small average sizes

- Before running large analytical queries on frequently-updated tables

**Important notes:**

- Compaction is memory-intensive; use `max_files` to limit batch size

- Files with different schema versions cannot be merged together

- Old files are not immediately deleted; run
  [`db_cleanup_files()`](https://cathalbyrnegit.github.io/datapond/reference/db_cleanup_files.md)
  after

## Usage

``` r
db_compact(schema = "main", table = NULL, max_files = NULL)
```

## Arguments

- schema:

  Schema name (default "main"). Use NULL to compact all schemas.

- table:

  Table name. Use NULL to compact all tables in the schema.

- max_files:

  Maximum number of files to compact in one operation. Lower values use
  less memory. Default NULL compacts all eligible files.

## Value

Invisibly returns a list with compaction results

## See also

[`db_file_stats()`](https://cathalbyrnegit.github.io/datapond/reference/db_file_stats.md)
to check file statistics before compacting,
[`db_cleanup_files()`](https://cathalbyrnegit.github.io/datapond/reference/db_cleanup_files.md)
to remove old files after compaction

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Check if compaction is needed
db_file_stats()

# Compact a specific table
db_compact(table = "imports")

# Compact with memory limit (process 500 files at a time)
db_compact(table = "imports", max_files = 500)

# Compact all tables in a schema
db_compact(schema = "trade")

# Compact entire catalog
db_compact()

# Clean up old files after compaction
db_cleanup_files()
} # }
```
