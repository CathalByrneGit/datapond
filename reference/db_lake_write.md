# Write a DuckLake table (overwrite/append)

Write a DuckLake table (overwrite/append)

## Usage

``` r
db_lake_write(
  data,
  schema = "main",
  table,
  mode = c("overwrite", "append"),
  partition_by = NULL,
  commit_author = NULL,
  commit_message = NULL
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

- partition_by:

  Optional character vector of column names to partition by. Only valid
  for mode = "overwrite". On overwrite, if not specified, existing
  partitioning is preserved.

- commit_author:

  Optional author for DuckLake commit metadata

- commit_message:

  Optional message for DuckLake commit metadata

## Value

Invisibly returns the qualified table name

## Examples

``` r
if (FALSE) { # \dontrun{
# Basic overwrite
db_lake_write(my_data, table = "imports")

# With schema
db_lake_write(my_data, schema = "trade", table = "imports")

# With partitioning (overwrite mode only)
db_lake_write(my_data, schema = "trade", table = "imports",
              partition_by = c("year", "month"))

# Append mode with commit info
db_lake_write(my_data, table = "imports", mode = "append",
              commit_author = "jsmith",
              commit_message = "Added Q3 data")
} # }
```
