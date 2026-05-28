# Get Iceberg metadata for a DuckLake table

Returns Iceberg-compatible metadata for a DuckLake table, including
schema, partitioning, and snapshot information in Iceberg format.

**Note:** This feature requires DuckLake functions that may not yet be
available in all versions. Check DuckLake documentation for
compatibility.

## Usage

``` r
db_iceberg_metadata(schema = "main", table)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

## Value

A list containing Iceberg metadata:

- `table_uuid`: Unique table identifier

- `schema`: Iceberg schema definition

- `partition_spec`: Partitioning specification

- `sort_order`: Sort order specification

- `current_snapshot_id`: Current snapshot ID

- `snapshots`: List of available snapshots

## See also

[`db_export_iceberg()`](https://cathalbyrnegit.github.io/datapond/reference/db_export_iceberg.md)
to export as Iceberg format

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Get Iceberg metadata
meta <- db_iceberg_metadata(table = "sales")
meta$schema
meta$partition_spec
} # }
```
