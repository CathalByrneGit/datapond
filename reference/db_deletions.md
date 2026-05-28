# Get deleted rows from the Data Change Feed

Convenience wrapper around
[`db_changes()`](https://cathalbyrnegit.github.io/datapond/reference/db_changes.md)
that returns only deleted rows. Uses DuckLake's native `table_deletions`
function.

## Usage

``` r
db_deletions(
  schema = "main",
  table,
  from_version,
  to_version = NULL,
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

- collect:

  If TRUE (default), returns collected data.frame. If FALSE, returns
  lazy tbl reference.

## Value

A data.frame with columns: snapshot_id, rowid, plus all table columns.

## See also

[`db_changes()`](https://cathalbyrnegit.github.io/datapond/reference/db_changes.md)
for all change types,
[`db_insertions()`](https://cathalbyrnegit.github.io/datapond/reference/db_insertions.md)
for inserted rows

## Examples

``` r
if (FALSE) { # \dontrun{
# Get all rows deleted between versions 1 and 5
removed_rows <- db_deletions(table = "products", from_version = 1, to_version = 5)
} # }
```
