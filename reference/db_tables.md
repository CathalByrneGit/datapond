# List tables in a DuckLake schema

Returns all tables in a given schema.

## Usage

``` r
db_tables(schema = "main")
```

## Arguments

- schema:

  Schema name (default "main")

## Value

Character vector of table names

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect()
db_tables()
# [1] "imports" "exports" "products"

db_tables("trade")
# [1] "monthly_summary" "annual_totals"
} # }
```
