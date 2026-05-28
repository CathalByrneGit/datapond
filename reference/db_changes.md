# Get row-level changes from the Data Change Feed

Uses DuckLake's native Data Change Feed to retrieve row-level changes
between snapshots. More efficient than
[`db_diff()`](https://cathalbyrnegit.github.io/datapond/reference/db_diff.md)
and provides detailed change types (insert, delete, update_preimage,
update_postimage).

## Usage

``` r
db_changes(
  schema = "main",
  table,
  from_version,
  to_version = NULL,
  change_types = c("insert", "delete", "update_preimage", "update_postimage"),
  collect = TRUE
)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- from_version:

  Starting snapshot version (inclusive)

- to_version:

  Ending snapshot version (inclusive, default: current)

- change_types:

  Character vector of change types to include. Options: "insert",
  "delete", "update_preimage", "update_postimage". Default includes all
  types.

- collect:

  If TRUE (default), returns collected data.frame. If FALSE, returns
  lazy tbl reference.

## Value

A data.frame with columns: snapshot_id, rowid, change_type, plus all
columns from the table.

## See also

[`db_diff()`](https://cathalbyrnegit.github.io/datapond/reference/db_diff.md)
for set-based comparison,
[`db_snapshots()`](https://cathalbyrnegit.github.io/datapond/reference/db_snapshots.md)
to list versions

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Get all changes between versions 1 and 5
changes <- db_changes(table = "products", from_version = 1, to_version = 5)

# See only inserts
inserts <- db_changes(table = "products", from_version = 1,
                      change_types = "insert")

# See updates (before and after)
updates <- db_changes(table = "products", from_version = 1,
                      change_types = c("update_preimage", "update_postimage"))
} # }
```
