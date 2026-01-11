# Preview an upsert operation

Shows what would happen if you ran
[`db_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_upsert.md) -
how many rows would be inserted vs updated.

## Usage

``` r
db_preview_upsert(data, schema = "main", table, by, update_cols = NULL)
```

## Arguments

- data:

  data.frame / tibble

- schema:

  Schema name (default "main")

- table:

  Table name

- by:

  Character vector of key columns used to match rows

- update_cols:

  Controls which columns to update on match:

  - NULL (default): update all columns

  - character(0): insert-only (no updates on match)

  - character vector: update only specified columns

## Value

A list with preview information (invisibly), also prints a summary

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect()

db_preview_upsert(my_data, table = "products", by = "product_id")
} # }
```
