# Search for columns by name

Find tables that contain columns matching a pattern.

## Usage

``` r
db_search_columns(pattern, schema = NULL)
```

## Arguments

- pattern:

  Column name pattern (case-insensitive)

- schema:

  Optional schema to limit search

## Value

A data.frame with schema, table, column_name, and column_type

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Find all columns with "country" in the name
db_search_columns("country")

# Find ID columns
db_search_columns("_id")
} # }
```
