# Get the data path for a table

Returns the data path configured for a DuckLake table. Returns NULL if
the table uses the default path (relative to schema).

## Usage

``` r
db_get_table_path(schema, table)
```

## Arguments

- schema:

  Schema name

- table:

  Table name

## Value

The path string, or NULL if using default

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect()
db_get_table_path("trade", "imports")
} # }
```
