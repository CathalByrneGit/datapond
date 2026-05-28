# Flush inlined data to parquet files

Writes inlined data (small inserts/deletes stored in the catalog) to
parquet files. DuckLake automatically inlines writes with fewer rows
than `data_inlining_row_limit` (default 10). Use this function to
consolidate inlined data into proper parquet files.

## Usage

``` r
db_flush_inlined(schema = "main", table = NULL)
```

## Arguments

- schema:

  Schema name (default "main"). Use NULL for all schemas.

- table:

  Table name. Use NULL for all tables in the schema.

## Value

Invisibly returns a data.frame with schema_name, table_name,
rows_flushed

## See also

[`db_set_inline_threshold()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_inline_threshold.md)
to configure inlining threshold

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Small writes are automatically inlined
db_write(small_batch, table = "events", mode = "append")  # < 10 rows

# Flush inlined data for a specific table
db_flush_inlined(table = "events")

# Flush all inlined data in a schema
db_flush_inlined(schema = "raw", table = NULL)

# Flush all inlined data in catalog
db_flush_inlined(schema = NULL, table = NULL)
} # }
```
