# Rollback a table to a previous snapshot

Restores a table to its state at a specific snapshot version or
timestamp. This creates a new snapshot with the rolled-back data.

## Usage

``` r
db_rollback(
  schema = "main",
  table,
  version = NULL,
  timestamp = NULL,
  commit_author = NULL,
  commit_message = NULL
)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- version:

  Snapshot version to rollback to (integer)

- timestamp:

  Timestamp to rollback to (POSIXct or character string)

- commit_author:

  Optional author for the rollback commit

- commit_message:

  Optional message for the rollback commit (defaults to auto-generated)

## Value

Invisibly returns the qualified table name

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Rollback to a specific version
db_rollback(table = "products", version = 5)

# Rollback to a specific time
db_rollback(table = "products", timestamp = "2025-01-15 00:00:00")
} # }
```
