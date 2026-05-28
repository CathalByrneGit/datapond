# Read a DuckLake table (lazy)

Read a DuckLake table (lazy)

## Usage

``` r
db_read(schema = "main", table, version = NULL, timestamp = NULL)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- version:

  Optional integer snapshot version for time travel

- timestamp:

  Optional timestamp string for time travel (e.g. "2025-05-26 00:00:00")

## Value

A lazy tbl_duckdb object

## Examples

``` r
if (FALSE) { # \dontrun{
# Basic read
db_read(table = "imports")

# From a specific schema
db_read(schema = "trade", table = "imports")

# Time travel by version
db_read(table = "imports", version = 5)

# Time travel by timestamp
db_read(table = "imports", timestamp = "2025-05-26 00:00:00")
} # }
```
