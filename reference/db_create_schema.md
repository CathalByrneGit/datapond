# Create a new schema in DuckLake

Creates a new schema in the DuckLake catalog. In DuckLake 0.2+, schemas
can have custom data paths, enabling folder-based access control.

## Usage

``` r
db_create_schema(schema)
```

## Arguments

- schema:

  Schema name to create

- path:

  Optional data path for this schema. Files for tables in this schema
  will be stored under this path. Use this to enable folder-based access
  control (e.g., different teams have access to different paths).

## Value

Invisibly returns the schema name

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect()

# Simple schema (uses default data path)
db_create_schema("reference")

# Schema with custom path for access control
db_create_schema("trade", path = "//CSO-NAS/DataLake/trade/")
db_create_schema("labour", path = "//CSO-NAS/DataLake/labour/")

# Now folder ACLs on //CSO-NAS/DataLake/trade/ control access to trade schema
} # }
```
