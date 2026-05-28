# Connect to a DuckLake data lake

Establishes a connection to DuckDB and attaches a DuckLake catalog.
DuckLake provides ACID transactions, time travel, and schema evolution
on top of Parquet files.

## Usage

``` r
db_connect(
  duckdb_db = ":memory:",
  catalog = "cso",
  catalog_type = NULL,
  metadata_path = "metadata.ducklake",
  data_path = "//CSO-NAS/DataLake",
  quack_token = NULL,
  snapshot_version = NULL,
  snapshot_time = NULL,
  encrypted = FALSE,
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

  - "quack://" URI -\> "quack" (EXPERIMENTAL)

  Can also be set explicitly to one of: "duckdb", "sqlite", "postgres",
  "quack".

  Note: "quack" uses the Quack Remote Protocol for client-server DuckDB
  with multiple concurrent writers. This is EXPERIMENTAL (beta in DuckDB
  1.5.x, production-ready version planned for DuckDB 2.0 in Fall 2026).

- metadata_path:

  Path or connection string for DuckLake metadata:

  - For "duckdb": file path (e.g. "metadata.ducklake")

  - For "sqlite": file path (e.g. "//CSO-NAS/DataLake/catalog.sqlite")

  - For "postgres": connection string (e.g. "dbname=ducklake_catalog
    host=localhost")

  - For "quack": URI (e.g. "quack:server:9494/catalog.ducklake")

- data_path:

  Root storage path where DuckLake writes Parquet data files

- quack_token:

  Authentication token for Quack server. If NULL (default), falls back
  to the `QUACK_TOKEN` environment variable. Only used when
  `catalog_type = "quack"`.

- snapshot_version:

  Optional integer snapshot version to attach at

- snapshot_time:

  Optional timestamp string to attach at (e.g. "2025-05-26 00:00:00")

- encrypted:

  If TRUE, enables encryption for all data files written to the data
  path. DuckLake auto-generates unique AES encryption keys per parquet
  file and stores them in the catalog. This enables zero-trust data
  hosting where data files can reside on untrusted storage. Keys are
  automatically retrieved from the catalog when reading encrypted files.

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
db_connect(
  metadata_path = "metadata.ducklake",
  data_path = "//CSO-NAS/DataLake"
)

# SQLite catalog (auto-detected from .sqlite extension)
db_connect(
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# PostgreSQL catalog (multi-user lakehouse, remote clients)
db_connect(
  catalog_type = "postgres",
  metadata_path = "dbname=ducklake_catalog host=db.cso.ie user=analyst",
  data_path = "//CSO-NAS/DataLake/data"
)

# Time travel - connect to a specific snapshot
db_connect(
  metadata_path = "catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data",
  snapshot_version = 5
)

# EXPERIMENTAL: Quack remote protocol (multi-writer via DuckDB server)
# Requires a DuckDB server running with Quack enabled
db_connect(
  catalog_type = "quack",
  metadata_path = "quack:db-server.cso.ie:9494/catalog.ducklake",
  data_path = "//CSO-NAS/DataLake/data",
  quack_token = "my-secret-token"
)

# Or use environment variable
Sys.setenv(QUACK_TOKEN = "my-secret-token")
db_connect(
  catalog_type = "quack",
  metadata_path = "quack:db-server.cso.ie:9494/catalog.ducklake",
  data_path = "//CSO-NAS/DataLake/data"
)

# Encrypted mode - zero-trust data hosting
# Keys are auto-generated per file and stored in the catalog
db_connect(
  metadata_path = "//secure-server/catalog.sqlite",
  data_path = "//untrusted-storage/data",
  encrypted = TRUE
)
} # }
```
