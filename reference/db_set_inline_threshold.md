# Set the inline threshold for a table, schema, or globally

Configures the row count threshold below which DuckLake automatically
inlines small writes to the catalog instead of writing parquet files.
The setting is persisted in DuckLake metadata.

## Usage

``` r
db_set_inline_threshold(schema = "main", table = NULL, threshold = 10)
```

## Arguments

- schema:

  Schema name (default "main"). Use NULL for global setting.

- table:

  Table name. Use NULL for schema-level or global setting.

- threshold:

  Number of rows threshold for inlining. Writes with fewer rows than
  this are inlined. Use 0 to disable inlining. Default is 10.

## Value

Invisibly returns TRUE on success

## See also

[`db_flush_inlined()`](https://cathalbyrnegit.github.io/datapond/reference/db_flush_inlined.md)
to manually flush inlined data

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Set threshold for a specific table
db_set_inline_threshold(table = "events", threshold = 50)

# Set threshold for entire schema
db_set_inline_threshold(schema = "raw", table = NULL, threshold = 100)

# Set global threshold
db_set_inline_threshold(schema = NULL, table = NULL, threshold = 20)

# Disable inlining for a table (all writes go directly to parquet)
db_set_inline_threshold(table = "events", threshold = 0)
} # }
```
