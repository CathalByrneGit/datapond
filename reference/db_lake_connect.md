# Connect to DuckDB + attach a DuckLake catalog

Connect to DuckDB + attach a DuckLake catalog

## Usage

``` r
db_lake_connect(
  duckdb_db = ":memory:",
  catalog = "cso",
  catalog_type = NULL,
  metadata_path = "metadata.ducklake",
  data_path = "//CSO-NAS/DataLake",
  snapshot_version = NULL,
  snapshot_time = NULL,
  threads = NULL,
  memory_limit = NULL,
  load_extensions = NULL
)
```

## Arguments

- duckdb_db:

  DuckDB database file path. Use ":memory:" for in-memory.

- catalog:

  DuckLake catalog name inside DuckDB (e.g. "cso")

- catalog_type:

  Type of catalog database backend. If NULL (default), auto-detected
  from metadata_path extension:

  - ".sqlite" or ".db" -\> "sqlite"

  - ".ducklake" or ".duckdb" -\> "duckdb"

  - "postgres://" connection string -\> "postgres"

  Can also be set explicitly to one of: "duckdb", "sqlite", "postgres".

- metadata_path:

  Path or connection string for DuckLake metadata:

  - For "duckdb": file path (e.g. "metadata.ducklake")

  - For "sqlite": file path (e.g. "//CSO-NAS/DataLake/catalog.sqlite")

  - For "postgres": connection string (e.g. "dbname=ducklake_catalog
    host=localhost")

- data_path:

  Root storage path where DuckLake writes Parquet data files

- snapshot_version:

  Optional integer snapshot version to attach at

- snapshot_time:

  Optional timestamp string to attach at (e.g. "2025-05-26 00:00:00")

- threads:

  Number of DuckDB threads (NULL leaves default)

- memory_limit:

  e.g. "4GB" (NULL leaves default)

- load_extensions:

  character vector of extensions to install/load, e.g. c("httpfs")

## Value

DuckDB connection object

## Examples

``` r
if (FALSE) { # \dontrun{
# DuckDB catalog (single user, simplest setup)
db_lake_connect(
  metadata_path = "metadata.ducklake",
  data_path = "//CSO-NAS/DataLake"
)

# SQLite catalog (auto-detected from .sqlite extension)
db_lake_connect(
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# PostgreSQL catalog (multi-user lakehouse, remote clients)
db_lake_connect(
  catalog_type = "postgres",
  metadata_path = "dbname=ducklake_catalog host=db.cso.ie user=analyst",
  data_path = "//CSO-NAS/DataLake/data"
)

# Time travel - connect to a specific snapshot
db_lake_connect(
  metadata_path = "catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data",
  snapshot_version = 5
)
} # }
```
