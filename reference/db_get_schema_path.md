# Get the data path for a schema

Returns the data path configured for a DuckLake schema. Returns NULL if
the schema uses the default catalog data path.

## Usage

``` r
db_get_schema_path(schema)
```

## Arguments

- schema:

  Schema name

## Value

The path string, or NULL if using default

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect()
db_create_schema("trade", path = "//CSO-NAS/trade/")
db_get_schema_path("trade")
#> "//CSO-NAS/trade/"
} # }
```
