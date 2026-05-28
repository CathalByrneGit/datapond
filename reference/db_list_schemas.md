# List schemas in the DuckLake catalog

Returns all schemas in the connected DuckLake catalog.

## Usage

``` r
db_list_schemas()
```

## Value

Character vector of schema names

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()
db_list_schemas()
# [1] "main" "trade" "labour"
} # }
```
