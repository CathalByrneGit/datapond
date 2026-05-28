# Get file statistics for DuckLake tables

Returns information about the Parquet files backing each table,
including file counts, sizes, and row counts. Use this to identify
tables that would benefit from compaction.

**Indicators that compaction may help:**

- High file count with small average file size (\< 10 MB)

- Many more files than expected for the data volume

- Slow query performance on tables with many files

## Usage

``` r
db_file_stats(schema = NULL, table = NULL)
```

## Arguments

- schema:

  Schema name (default "main"). Use NULL for all schemas.

- table:

  Table name. Use NULL for all tables.

## Value

A data.frame with columns:

- `schema_name`: Schema containing the table

- `table_name`: Table name

- `file_count`: Number of Parquet files

- `total_rows`: Total row count across all files

- `total_bytes`: Total size in bytes

- `avg_file_bytes`: Average file size

- `avg_rows_per_file`: Average rows per file

## See also

[`db_compact()`](https://cathalbyrnegit.github.io/datapond/reference/db_compact.md)
to merge small files

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Check all tables
db_file_stats()

# Check a specific table
db_file_stats(table = "imports")

# Find tables needing compaction (many small files)
stats <- db_file_stats()
stats[stats$file_count > 100 & stats$avg_file_bytes < 1e7, ]
} # }
```
