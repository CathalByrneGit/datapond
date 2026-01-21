# Create a new schema in DuckLake

Creates a new schema in the DuckLake catalog.

DuckLake automatically organizes data into `{schema}/{table}/` folders
under the catalog's DATA_PATH. This default structure enables
folder-based access control - simply set ACLs on the schema folders.

## Usage

``` r
db_create_schema(schema)
```

## Arguments

- schema:

  Schema name to create

## Value

Invisibly returns the schema name

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect(data_path = "//CSO-NAS/DataLake")

db_create_schema("trade")
db_create_schema("labour")

# Data will be organized as:
# //CSO-NAS/DataLake/trade/imports/ducklake-xxx.parquet
# //CSO-NAS/DataLake/trade/exports/ducklake-xxx.parquet
# //CSO-NAS/DataLake/labour/employment/ducklake-xxx.parquet

# Set folder ACLs on //CSO-NAS/DataLake/trade/ to control access
} # }
```
