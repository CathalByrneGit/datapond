# Write an Arrow Table to DuckLake

Writes an Arrow Table or RecordBatch directly to DuckLake. This provides
an alternative to
[`db_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_write.md)
when your data is already in Arrow format.

## Usage

``` r
db_write_arrow(
  data,
  schema = "main",
  table,
  mode = c("overwrite", "append"),
  commit_author = NULL,
  commit_message = NULL
)
```

## Arguments

- data:

  An Arrow Table, RecordBatch, or data.frame

- schema:

  Schema name (default "main")

- table:

  Table name

- mode:

  "overwrite" or "append"

- commit_author:

  Optional author for DuckLake commit metadata

- commit_message:

  Optional message for DuckLake commit metadata

## Value

Invisibly returns the qualified table name

## See also

[`db_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_write.md)
for writing data.frames with more options,
[`db_read_arrow()`](https://cathalbyrnegit.github.io/datapond/reference/db_read_arrow.md)
for reading as Arrow

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect(...)

# Write Arrow Table
arrow_tbl <- arrow::arrow_table(id = 1:3, value = c(10, 20, 30))
db_write_arrow(arrow_tbl, table = "metrics")

# Write from parquet file
arrow_tbl <- arrow::read_parquet("data.parquet")
db_write_arrow(arrow_tbl, table = "imports", mode = "append")
} # }
```
