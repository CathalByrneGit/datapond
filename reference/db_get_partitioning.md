# Get partitioning configuration for a DuckLake table

Returns the current partition keys configured for a table, or NULL if
the table is not partitioned.

## Usage

``` r
db_get_partitioning(schema = "main", table)
```

## Arguments

- schema:

  Schema name

- table:

  Table name

## Value

A character vector of partition key expressions, or NULL if not
partitioned

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect(...)
db_set_partitioning("trade", "imports", c("year", "month"))
db_get_partitioning("trade", "imports")
#> [1] "year" "month"
} # }
```
