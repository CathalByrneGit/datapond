# Export a DuckLake table as Iceberg format (EXPERIMENTAL)

Exports a DuckLake table to Iceberg format for compatibility with other
data lakehouse engines (Spark, Trino, Presto, etc.).

**Note:** This is experimental. DuckLake 0.3+ supports Iceberg
interoperability via `COPY FROM DATABASE ducklake TO iceberg_catalog`.
This function attempts to use internal DuckLake Iceberg functions which
may not be available.

## Usage

``` r
db_export_iceberg(
  schema = "main",
  table,
  path = NULL,
  catalog_type = c("hadoop", "hive", "rest")
)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- path:

  Output path for Iceberg metadata. If NULL, uses the table's existing
  data path with an `iceberg/` subdirectory.

- catalog_type:

  Type of Iceberg catalog to generate: "hadoop" (default), "hive", or
  "rest".

## Value

Invisibly returns the output path

## See also

[`db_iceberg_metadata()`](https://cathalbyrnegit.github.io/datapond/reference/db_iceberg_metadata.md)
to view Iceberg metadata

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Export to Iceberg format
db_export_iceberg(table = "sales")

# Export to specific location
db_export_iceberg(table = "sales", path = "/data/iceberg/sales")

# Export for Hive Metastore compatibility
db_export_iceberg(table = "sales", catalog_type = "hive")
} # }
```
