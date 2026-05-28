# Check if a DuckLake table exists

Check if a DuckLake table exists

## Usage

``` r
db_table_exists(schema = "main", table)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

## Value

Logical TRUE if exists, FALSE otherwise

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()
db_table_exists(table = "imports")
# [1] TRUE
} # }
```
