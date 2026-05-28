# Clean up orphaned files from DuckLake storage

Removes data files that are no longer referenced by any snapshot. Run
this after
[`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md)
or
[`db_compact()`](https://cathalbyrnegit.github.io/datapond/reference/db_compact.md)
to reclaim disk space.

**When files become orphaned:**

- After
  [`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md)
  removes old snapshots

- After
  [`db_compact()`](https://cathalbyrnegit.github.io/datapond/reference/db_compact.md)
  merges files (old small files become orphaned)

- After failed transactions that wrote partial data

## Usage

``` r
db_cleanup_files(dry_run = TRUE)
```

## Arguments

- dry_run:

  If TRUE (default), shows what would be deleted without deleting. Set
  to FALSE to actually remove files.

## Value

Invisibly returns the count of files cleaned up (or that would be)

## See also

[`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md)
to remove old snapshots,
[`db_compact()`](https://cathalbyrnegit.github.io/datapond/reference/db_compact.md)
to merge files

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Compact files then clean up
db_compact(table = "imports")
db_cleanup_files(dry_run = FALSE)

# Vacuum old snapshots then clean up
db_vacuum(older_than = "30 days", dry_run = FALSE)
db_cleanup_files(dry_run = FALSE)
} # }
```
