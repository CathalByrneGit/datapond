# Preview a DuckLake write operation

Shows what would happen if you ran
[`db_lake_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_lake_write.md)
without actually writing any data.

## Usage

``` r
db_preview_lake_write(
  data,
  schema = "main",
  table,
  mode = c("overwrite", "append")
)
```

## Arguments

- data:

  data.frame/tibble

- schema:

  Schema name (default "main")

- table:

  Table name

- mode:

  "overwrite" or "append"

## Value

A list with preview information (invisibly), also prints a summary

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect()

db_preview_lake_write(my_data, table = "products", mode = "overwrite")
} # }
```
