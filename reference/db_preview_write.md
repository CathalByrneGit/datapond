# Preview a DuckLake write operation

Shows what would happen if you ran
[`db_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_write.md)
without actually writing any data.

## Usage

``` r
db_preview_write(
  data,
  schema = "main",
  table,
  mode = c("overwrite", "append"),
  partition_by = NULL
)
```

## Arguments

- data:

  A data.frame, tibble, or lazy dbplyr table (from
  [`db_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_read.md)
  or `tbl()`). Lazy tables enable zero-copy transformations within
  DuckDB.

- schema:

  Schema name (default "main")

- table:

  Table name

- mode:

  "overwrite" or "append"

- partition_by:

  Optional character vector of column names to partition by. Only valid
  for mode = "overwrite". On overwrite, if not specified, existing
  partitioning is preserved.

## Value

A list with preview information (invisibly), also prints a summary

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

db_preview_write(my_data, table = "products", mode = "overwrite")

# Preview with partitioning
db_preview_write(my_data, table = "sales",
                 partition_by = c("year", "month"))
} # }
```
