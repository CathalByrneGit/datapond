# Get inserted rows from the Data Change Feed

Convenience wrapper around
[`db_changes()`](https://cathalbyrnegit.github.io/datapond/reference/db_changes.md)
that returns only inserted rows. Uses DuckLake's native
`table_insertions` function.

## Usage

``` r
db_insertions(
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
[`db_deletions()`](https://cathalbyrnegit.github.io/datapond/reference/db_deletions.md)
for deleted rows

## Examples

``` r
if (FALSE) { # \dontrun{
# Get all rows inserted between versions 1 and 5
new_rows <- db_insertions(table = "products", from_version = 1, to_version = 5)
} # }
```
