# Compare a table between two snapshots

Shows the differences in a table between two snapshot versions db_diff()
is set-based (EXCEPT), so duplicates don't count as "added". Your append
produced 10 duplicate rows + 2 genuinely new distinct rows, so it
reports 2 added. or timestamps. Returns added, removed, and (optionally)
changed rows.

## Usage

``` r
db_diff(
  schema = "main",
  table,
  from_version = NULL,
  to_version = NULL,
  from_timestamp = NULL,
  to_timestamp = NULL,
  key_cols = NULL,
  collect = TRUE
)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- from_version:

  Starting snapshot version (integer) or NULL to use from_timestamp

- to_version:

  Ending snapshot version (integer, default: current) or NULL to use
  to_timestamp

- from_timestamp:

  Starting timestamp (alternative to from_version)

- to_timestamp:

  Ending timestamp (alternative to to_version, default: current)

- key_cols:

  Character vector of columns that uniquely identify rows. If provided,
  enables detection of modified rows (not just added/removed).

- collect:

  If TRUE (default), returns collected data.frames. If FALSE, returns
  lazy tbl references.

## Value

A list with components: `added`, `removed`, and (if key_cols provided)
`modified`

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Compare versions 3 and 5
diff <- db_diff(table = "products", from_version = 3, to_version = 5)
diff$added
diff$removed

# Compare with key columns to see modifications
diff <- db_diff(table = "products", from_version = 3, to_version = 5,
                key_cols = "product_id")
diff$modified
} # }
```
