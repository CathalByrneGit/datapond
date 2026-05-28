# Enable DuckDB/DuckLake logging

Enables logging for debugging and monitoring DuckLake operations.
DuckLake registers a dedicated log type for metadata queries. The
built-in QueryLog type can trace all SQL queries including internal
ones.

## Usage

``` r
db_enable_logging(enable = TRUE, log_type = c("query", "metadata", "all"))
```

## Arguments

- enable:

  If TRUE, enable logging. If FALSE, disable.

- log_type:

  Type of logging: "query" for SQL queries, "metadata" for DuckLake
  metadata operations, or "all" for both. Default "query".

## Value

Invisibly returns TRUE on success

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect(...)

# Enable query logging
db_enable_logging(TRUE)

# Run some operations...
db_read(table = "users") |> head()

# View logs
db_query("SELECT * FROM duckdb_logs() ORDER BY timestamp DESC LIMIT 20")

# Disable logging
db_enable_logging(FALSE)
} # }
```
