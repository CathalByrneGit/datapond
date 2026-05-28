# Vacuum old snapshots from DuckLake

Removes old snapshots and their associated data files that are no longer
needed. This reclaims storage space by deleting data that is not
referenced by any snapshot within the retention period.

## Usage

``` r
db_vacuum(older_than = "30 days", dry_run = TRUE)
```

## Arguments

- older_than:

  Snapshots older than this will be removed. Can be:

  - A difftime or lubridate duration (e.g.
    `as.difftime(7, units = "days")`)

  - A character string parseable by DuckDB (e.g. "7 days", "1 month")

  - A POSIXct timestamp (snapshots before this time are removed)

- dry_run:

  If TRUE (default), reports what would be deleted without actually
  deleting. Set to FALSE to perform the actual cleanup.

## Value

A data.frame summarising what was (or would be) cleaned up

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# See what would be cleaned up (dry run)
db_vacuum(older_than = "30 days")

# Actually clean up
db_vacuum(older_than = "30 days", dry_run = FALSE)
} # }
```
