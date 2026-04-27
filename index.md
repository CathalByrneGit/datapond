# datapond

**datapond** is a simple and lightweight data lake infrastructure for
small to medium data requirements. It provides a unified R interface for
[DuckLake](https://ducklake.select/) — a modern data lakehouse built on
[DuckDB](https://duckdb.org/) that adds ACID transactions, time travel,
and schema evolution on top of Parquet files.

## Installation

``` r
# Install from local source
devtools::install("path/to/datapond")

# Or load for development
devtools::load_all("path/to/datapond")
```

## Quick Start

``` r
library(datapond)

# Connect to DuckLake with SQLite catalog (recommended for shared drives)
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# See what's available
db_list_schemas()
#> [1] "main" "trade" "labour"

db_tables("trade")
#> [1] "imports" "exports" "products"

# Read current data (returns a lazy dplyr table)
imports <- db_read(schema = "trade", table = "imports")

imports |>
  filter(year == 2024) |>
  group_by(country) |>
  summarise(total = sum(value)) |>
  collect()

# Read data as it was at a specific version
imports_v5 <- db_read(schema = "trade", table = "imports", version = 5)

# Read data as it was at a specific time
imports_jan <- db_read(
  schema = "trade",
  table = "imports",
  timestamp = "2025-01-15 00:00:00"
)

# Preview write to see impact before executing
db_preview_write(my_data, schema = "trade", table = "imports", mode = "append")

# Write with commit metadata
db_write(
  my_data,
  schema = "trade",
  table = "imports",
  mode = "append",
  commit_author = "jsmith",
  commit_message = "Added Q1 2025 data"
)

# Write with partitioning for faster queries
db_write(
  my_data,
  schema = "trade",
  table = "imports",
  partition_by = c("year", "month")
)

# Bucket partitioning for high-cardinality columns (e.g., user IDs)
db_write(
  events_data,
  table = "events",
  bucket_by = list(column = "user_id", buckets = 16)
)

# Sorted/clustered tables for better range query performance
db_write(
  sales_data,
  table = "sales",
  sort_by = c("sale_date", "region")
)

# Streaming: inline small writes to avoid many small files
db_write(batch, table = "events", mode = "append", inline = TRUE)
# Then flush when ready
db_flush_inlined(table = "events")

# Preview upsert to see how many inserts vs updates
db_preview_upsert(my_data, schema = "trade", table = "products", by = "product_id")

# Upsert (update existing, insert new)
db_upsert(
  my_data,
  schema = "trade",
  table = "products",
  by = "product_id",
  commit_message = "Price updates"
)

# View snapshot history
db_snapshots()

# Compare versions
diff <- db_diff(schema = "trade", table = "imports",
                from_version = 5, to_version = 10)
diff$added
diff$removed

# Rollback if something went wrong
db_rollback(schema = "trade", table = "imports", version = 5)

# Clean up old snapshots
db_vacuum(older_than = "30 days", dry_run = FALSE)

# Check file statistics (useful before compaction)
db_file_stats()

# Compact small files into larger ones for better performance
db_compact(table = "imports")

# Remove orphaned files after vacuum or compact
db_cleanup_files(dry_run = FALSE)

# Export to Iceberg format for Spark/Trino compatibility
db_export_iceberg(table = "imports")

db_disconnect()
```

### Data Documentation & Discovery

``` r
library(datapond)
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# Document your tables
db_describe(
  table = "imports",
  description = "Monthly import values by country and commodity code",
  owner = "Trade Section",
  tags = c("trade", "monthly", "official")
)

# Document individual columns
db_describe_column(
  table = "imports",
  column = "value",
  description = "Import value in thousands",
  units = "EUR (thousands)"
)

# Search for tables
db_search("trade")
db_search("official", field = "tags")

# Find columns across all tables
db_search_columns("country")

# Generate a data dictionary
dict <- db_dictionary()
# Export to Excel
writexl::write_xlsx(dict, "data_dictionary.xlsx")
```

### Data Lineage

``` r
# Record where data came from
db_lineage(
  table = "monthly_summary",
  sources = c("raw.transactions", "raw.products"),
  transformation = "Aggregated by month and product category"
)

# Retrieve lineage information
db_get_lineage(table = "monthly_summary")
#> $sources
#> [1] "raw.transactions" "raw.products"
#>
#> $transformation
#> [1] "Aggregated by month and product category"
```

### Interactive Browser

``` r
library(datapond)
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# Launch interactive browser
db_browser()
```

The browser provides a point-and-click interface for:

- **Browse** - Navigate schemas and tables in a tree view
- **Preview** - View sample rows from any table
- **Metadata** - See documentation, owner, and tags
- **Search** - Find tables by name, description, or tags
- **Dictionary** - Generate and export a data dictionary

## Choosing a Catalog Backend

DuckLake stores metadata (table definitions, snapshots, file tracking)
in a **catalog database**. You can choose from three backends:

| Backend    | `catalog_type` | Concurrency              | Best For                   |
|------------|----------------|--------------------------|----------------------------|
| DuckDB     | `"duckdb"`     | Single client only       | Personal/dev use           |
| **SQLite** | `"sqlite"`     | Multi-read, single-write | **Shared network drives**  |
| PostgreSQL | `"postgres"`   | Full concurrent access   | Large teams, remote access |

### Recommended: SQLite for Shared Drives

``` r
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)
```

**Why SQLite?**

- Still just a file on the network drive (no server needed)
- Supports multiple people reading simultaneously
- Single-writer with automatic retry (handles realistic usage)
- Works with existing IT permissions model

### PostgreSQL for Production Scale

If you need true multi-user concurrent writes or remote access:

``` r
db_connect(
  catalog_type = "postgres",
  metadata_path = "dbname=ducklake_catalog host=db.cso.ie",
  data_path = "//CSO-NAS/DataLake/data"
)
```

## Access Control

datapond relies on **file system permissions** for access control.
DuckLake automatically organises data into `{schema}/{table}/` folders
within the data path, so you can grant permissions at the schema level.

``` r
# Create schemas - DuckLake creates data folders automatically
db_create_schema("trade")
db_create_schema("labour")

# Write data - files go to data/trade/imports/ automatically
db_write(imports_data, schema = "trade", table = "imports")

# Then grant folder ACLs:
# - data/trade/   → Trade team read/write
# - data/labour/  → Labour team read/write
```

## Function Reference

### Connection

| Function                                                                                  | Description                                                              |
|-------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`db_connect()`](https://cathalbyrnegit.github.io/datapond/reference/db_connect.md)       | Connect to a DuckLake catalog (supports duckdb/sqlite/postgres backends) |
| [`db_disconnect()`](https://cathalbyrnegit.github.io/datapond/reference/db_disconnect.md) | Close connection                                                         |
| [`db_status()`](https://cathalbyrnegit.github.io/datapond/reference/db_status.md)         | Show connection info (including catalog type)                            |

### Reading Data

| Function    | Description                                                    |
|-------------|----------------------------------------------------------------|
| `db_read()` | Read table (with optional time travel by version or timestamp) |

### Writing Data

| Function                                                                          | Description                                                                     |
|-----------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `db_write()`                                                                      | Write table (overwrite/append, with partitioning, bucketing, sorting, inlining) |
| [`db_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_upsert.md) | MERGE operation (update existing rows + insert new rows)                        |

### Preview Operations

| Function                                                                                          | Description                                 |
|---------------------------------------------------------------------------------------------------|---------------------------------------------|
| `db_preview_write()`                                                                              | Preview write impact before executing       |
| [`db_preview_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_upsert.md) | Preview inserts vs updates before executing |

### Discovery

| Function                                                                                        | Description             |
|-------------------------------------------------------------------------------------------------|-------------------------|
| [`db_list_schemas()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_schemas.md)   | List schemas            |
| [`db_tables()`](https://cathalbyrnegit.github.io/datapond/reference/db_tables.md)               | List tables in a schema |
| [`db_list_views()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_views.md)       | List views in a schema  |
| [`db_table_exists()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_exists.md)   | Check if a table exists |
| [`db_create_schema()`](https://cathalbyrnegit.github.io/datapond/reference/db_create_schema.md) | Create a new schema     |

### Documentation & Search

| Function                                                                                            | Description                                       |
|-----------------------------------------------------------------------------------------------------|---------------------------------------------------|
| [`db_describe()`](https://cathalbyrnegit.github.io/datapond/reference/db_describe.md)               | Add description, owner, tags to a table           |
| [`db_describe_column()`](https://cathalbyrnegit.github.io/datapond/reference/db_describe_column.md) | Document a column (description, units, notes)     |
| [`db_get_docs()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_docs.md)               | Retrieve documentation for a table                |
| [`db_dictionary()`](https://cathalbyrnegit.github.io/datapond/reference/db_dictionary.md)           | Generate full data dictionary                     |
| [`db_search()`](https://cathalbyrnegit.github.io/datapond/reference/db_search.md)                   | Search by name, description, owner, or tags       |
| [`db_search_columns()`](https://cathalbyrnegit.github.io/datapond/reference/db_search_columns.md)   | Find columns by name across all tables            |
| `db_lineage()`                                                                                      | Record data lineage (sources and transformations) |
| `db_get_lineage()`                                                                                  | Retrieve lineage information for a table          |

### Partitioning & Clustering

| Function                                                                                              | Description                                            |
|-------------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| [`db_set_partitioning()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_partitioning.md) | Set or remove partitioning on a table                  |
| [`db_get_partitioning()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_partitioning.md) | Get current partition columns for a table              |
| `db_set_clustering()`                                                                                 | Set sort/clustering order for better query performance |
| `db_recluster()`                                                                                      | Re-sort existing data to match clustering order        |

### Data Inlining (Streaming)

| Function                    | Description                                     |
|-----------------------------|-------------------------------------------------|
| `db_flush_inlined()`        | Write inlined data to parquet files             |
| `db_set_inline_threshold()` | Configure auto-flush threshold for inlined data |

### Iceberg Export

| Function                | Description                                           |
|-------------------------|-------------------------------------------------------|
| `db_export_iceberg()`   | Export table to Iceberg format for Spark/Trino/Presto |
| `db_iceberg_metadata()` | Get Iceberg-compatible metadata for a table           |

### Metadata & Maintenance

| Function                                                                                  | Description                                                 |
|-------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| [`db_snapshots()`](https://cathalbyrnegit.github.io/datapond/reference/db_snapshots.md)   | List all snapshots                                          |
| [`db_catalog()`](https://cathalbyrnegit.github.io/datapond/reference/db_catalog.md)       | Table info and stats                                        |
| [`db_table_cols()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_cols.md) | Get column names for a table                                |
| [`db_view_cols()`](https://cathalbyrnegit.github.io/datapond/reference/db_view_cols.md)   | Get column names for a view                                 |
| [`db_diff()`](https://cathalbyrnegit.github.io/datapond/reference/db_diff.md)             | Compare two snapshots                                       |
| [`db_rollback()`](https://cathalbyrnegit.github.io/datapond/reference/db_rollback.md)     | Restore table to a previous version                         |
| [`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md)         | Clean up old snapshots and unreferenced files               |
| `db_compact()`                                                                            | Merge small Parquet files for better performance            |
| `db_file_stats()`                                                                         | Get file counts and sizes to identify compaction candidates |
| `db_cleanup_files()`                                                                      | Remove orphaned files after vacuum or compact               |
| [`db_query()`](https://cathalbyrnegit.github.io/datapond/reference/db_query.md)           | Run arbitrary SQL                                           |

### Interactive Tools

| Function                                                                              | Description                                             |
|---------------------------------------------------------------------------------------|---------------------------------------------------------|
| [`db_browser()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser.md)   | Launch interactive Shiny browser for exploring data     |
| [`run_example()`](https://cathalbyrnegit.github.io/datapond/reference/run_example.md) | Run bundled example scripts (call without args to list) |

## Learn More

- [`vignette("concepts")`](https://cathalbyrnegit.github.io/datapond/articles/concepts.md) -
  Background on DuckLake, catalog backends, and access control
- [`vignette("code-walkthrough")`](https://cathalbyrnegit.github.io/datapond/articles/code-walkthrough.md) -
  Detailed explanation of how the package works

## Contributing

Found a bug or have a feature request? Please open an issue on GitHub.
