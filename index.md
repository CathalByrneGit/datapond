# datapond

**datapond** is a simple and ligthweight data lake infrastructure for
small to medium data requirements. Provides a unified R interface for
duckdb and ducklake internal data infrastructure. It supports two
storage backends:

1.  **Hive-partitioned Parquet** - familiar folder-based structure
    (similar to existing SAS storage)
2.  **DuckLake** - modern data lakehouse with time travel, schema
    evolution, and ACID transactions

Both backends use [DuckDB](https://duckdb.org/) as the query engine,
giving you fast analytical queries without needing a server.

## Installation

``` r
# Install from local source
devtools::install("path/to/datapond")

# Or load for development
devtools::load_all("path/to/datapond")
```

## Quick Start

### Hive Mode (Folder-based)

``` r
library(datapond)

# Connect to the data lake
db_connect(path = "//CSO-NAS/DataLake")

# See what's available
db_list_sections()
#> [1] "Trade" "Labour" "Health" "Agriculture"

db_list_datasets("Trade")
#> [1] "Imports" "Exports" "Balance"

# Read a dataset (returns a lazy dplyr table)
imports <- db_hive_read("Trade", "Imports")

# Work with it using dplyr
imports |>
  filter(year == 2024) |>
  group_by(country) |>
  summarise(total = sum(value)) |>
  collect()

# Preview before writing (see what will happen)
db_preview_hive_write(
  my_data,
  section = "Trade",
  dataset = "Imports",
  partition_by = c("year", "month"),
  mode = "replace_partitions"
)

# Write data (partitioned by year and month)
db_hive_write(
  my_data, 
  section = "Trade", 
  dataset = "Imports",
  partition_by = c("year", "month"),
  mode = "replace_partitions"
)

# Disconnect when done
db_disconnect()
```

### DuckLake Mode (Time Travel)

``` r
library(datapond)

# Connect to DuckLake with SQLite catalog (recommended for shared drives)
db_lake_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# See what's available
db_list_schemas()
#> [1] "main" "trade" "labour"

db_list_tables("trade")
#> [1] "imports" "exports" "products"

# Read current data
imports <- db_lake_read(schema = "trade", table = "imports")

# Read data as it was at a specific version
imports_v5 <- db_lake_read(schema = "trade", table = "imports", version = 5)

# Read data as it was at a specific time
imports_jan <- db_lake_read(
  schema = "trade", 
  table = "imports", 
  timestamp = "2025-01-15 00:00:00"
)

# Preview write to see impact
db_preview_lake_write(my_data, schema = "trade", table = "imports", mode = "append")

# Write with commit metadata
db_lake_write(
  my_data,
  schema = "trade",
  table = "imports",
  mode = "append",
  commit_author = "jsmith",
  commit_message = "Added Q1 2025 data"
)

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

db_disconnect()
```

### Data Documentation & Discovery

``` r
library(datapond)
db_connect(path = "//CSO-NAS/DataLake")

# Document your datasets
db_describe(
  section = "Trade",
  dataset = "Imports",
  description = "Monthly import values by country and commodity code",
  owner = "Trade Section",
  tags = c("trade", "monthly", "official")
)

# Document individual columns
db_describe_column(
  section = "Trade",
  dataset = "Imports",
  column = "value",
  description = "Import value in thousands",
  units = "EUR (thousands)"
)

# Search for datasets
db_search("trade")
db_search("official", field = "tags")

# Find columns across all datasets
db_search_columns("country")

# Generate a data dictionary
dict <- db_dictionary()
# Export to Excel
writexl::write_xlsx(dict, "data_dictionary.xlsx")
```

### Interactive Browser

``` r
library(datapond)
db_connect(path = "//CSO-NAS/DataLake")

# Launch interactive browser
db_browser()
```

The browser provides a point-and-click interface for:

- **Browse** - Navigate sections/datasets or schemas/tables in a tree
  view
- **Preview** - View sample rows from any dataset
- **Metadata** - See documentation, owner, and tags
- **Search** - Find datasets by name, description, or tags
- **Dictionary** - Generate and export a data dictionary

## Why Two Modes?

| Feature           | Hive Mode                          | DuckLake Mode                      |
|-------------------|------------------------------------|------------------------------------|
| Storage           | Folders + Parquet files            | Managed Parquet + metadata catalog |
| Time travel       | ❌                                 | ✅ Version and timestamp queries   |
| Schema evolution  | Manual                             | ✅ Automatic                       |
| ACID transactions | ❌                                 | ✅                                 |
| Familiarity       | Similar to SAS libraries           | New paradigm                       |
| Best for          | Simple sharing, migration from SAS | Production data pipelines          |

**Start with Hive mode** if you’re migrating from SAS or need something
familiar. **Use DuckLake mode** when you need versioning, rollback, or
proper transaction support.

## Choosing a Catalog Backend (DuckLake)

DuckLake stores metadata (table definitions, snapshots, file tracking)
in a **catalog database**. You can choose from three backends:

| Backend    | `catalog_type` | Concurrency              | Best For                   |
|------------|----------------|--------------------------|----------------------------|
| DuckDB     | `"duckdb"`     | Single client only       | Personal/dev use           |
| **SQLite** | `"sqlite"`     | Multi-read, single-write | **Shared network drives**  |
| PostgreSQL | `"postgres"`   | Full concurrent access   | Large teams, remote access |

### Recommended: SQLite for CSO

For most CSO use cases with shared network drives, use SQLite:

``` r
db_lake_connect(
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
db_lake_connect(
  catalog_type = "postgres",
  metadata_path = "dbname=ducklake_catalog host=db.cso.ie",
  data_path = "//CSO-NAS/DataLake/data"
)
```

## Access Control

Both modes rely on **file system permissions** for access control:

- **Hive mode**: Users need read/write access to the relevant section
  folders
- **DuckLake mode**: Users need access to both the metadata
  file/database and data folder

This integrates with existing IT infrastructure - no new authentication
systems needed.

## Function Reference

### Connection

| Function                                                                                      | Description                                                         |
|-----------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| [`db_connect()`](https://cathalbyrnegit.github.io/datapond/reference/db_connect.md)           | Connect in hive mode                                                |
| [`db_lake_connect()`](https://cathalbyrnegit.github.io/datapond/reference/db_lake_connect.md) | Connect in DuckLake mode (supports duckdb/sqlite/postgres catalogs) |
| [`db_disconnect()`](https://cathalbyrnegit.github.io/datapond/reference/db_disconnect.md)     | Close connection                                                    |
| [`db_status()`](https://cathalbyrnegit.github.io/datapond/reference/db_status.md)             | Show connection info (including catalog type)                       |

### Reading Data

| Function                                                                                | Mode     | Description                            |
|-----------------------------------------------------------------------------------------|----------|----------------------------------------|
| [`db_hive_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_hive_read.md) | Hive     | Read partitioned parquet dataset       |
| [`db_lake_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_lake_read.md) | DuckLake | Read table (with optional time travel) |

### Writing Data

| Function                                                                                  | Mode     | Description                                                     |
|-------------------------------------------------------------------------------------------|----------|-----------------------------------------------------------------|
| [`db_hive_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_hive_write.md) | Hive     | Write partitioned parquet (overwrite/append/replace_partitions) |
| [`db_lake_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_lake_write.md) | DuckLake | Write table (overwrite/append)                                  |
| [`db_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_upsert.md)         | DuckLake | MERGE operation (update + insert)                               |

### Preview Operations

| Function                                                                                                  | Mode     | Description                                 |
|-----------------------------------------------------------------------------------------------------------|----------|---------------------------------------------|
| [`db_preview_hive_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_hive_write.md) | Hive     | Preview write impact before executing       |
| [`db_preview_lake_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_lake_write.md) | DuckLake | Preview write impact before executing       |
| [`db_preview_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_upsert.md)         | DuckLake | Preview inserts vs updates before executing |

### Discovery

| Function                                                                                          | Mode     | Description                |
|---------------------------------------------------------------------------------------------------|----------|----------------------------|
| [`db_list_sections()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_sections.md)   | Hive     | List top-level sections    |
| [`db_list_datasets()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_datasets.md)   | Hive     | List datasets in a section |
| [`db_dataset_exists()`](https://cathalbyrnegit.github.io/datapond/reference/db_dataset_exists.md) | Hive     | Check if dataset exists    |
| [`db_list_schemas()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_schemas.md)     | DuckLake | List schemas               |
| [`db_list_tables()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_tables.md)       | DuckLake | List tables in schema      |
| [`db_list_views()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_views.md)         | DuckLake | List views in schema       |
| [`db_table_exists()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_exists.md)     | DuckLake | Check if table exists      |
| [`db_create_schema()`](https://cathalbyrnegit.github.io/datapond/reference/db_create_schema.md)   | DuckLake | Create a new schema        |

### Documentation & Search

| Function                                                                                            | Mode | Description                                   |
|-----------------------------------------------------------------------------------------------------|------|-----------------------------------------------|
| [`db_describe()`](https://cathalbyrnegit.github.io/datapond/reference/db_describe.md)               | Both | Add description, owner, tags to dataset/table |
| [`db_describe_column()`](https://cathalbyrnegit.github.io/datapond/reference/db_describe_column.md) | Both | Document a column (description, units, notes) |
| [`db_get_docs()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_docs.md)               | Both | Retrieve documentation for a dataset/table    |
| [`db_dictionary()`](https://cathalbyrnegit.github.io/datapond/reference/db_dictionary.md)           | Both | Generate full data dictionary                 |
| [`db_search()`](https://cathalbyrnegit.github.io/datapond/reference/db_search.md)                   | Both | Search by name, description, owner, or tags   |
| [`db_search_columns()`](https://cathalbyrnegit.github.io/datapond/reference/db_search_columns.md)   | Both | Find columns by name across all datasets      |

### Metadata & Maintenance

| Function                                                                                  | Mode     | Description                 |
|-------------------------------------------------------------------------------------------|----------|-----------------------------|
| [`db_snapshots()`](https://cathalbyrnegit.github.io/datapond/reference/db_snapshots.md)   | DuckLake | List all snapshots          |
| [`db_catalog()`](https://cathalbyrnegit.github.io/datapond/reference/db_catalog.md)       | DuckLake | Table info and stats        |
| [`db_table_cols()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_cols.md) | DuckLake | Get column names            |
| [`db_diff()`](https://cathalbyrnegit.github.io/datapond/reference/db_diff.md)             | DuckLake | Compare snapshots           |
| [`db_rollback()`](https://cathalbyrnegit.github.io/datapond/reference/db_rollback.md)     | DuckLake | Restore to previous version |
| [`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md)         | DuckLake | Clean up old snapshots      |
| [`db_query()`](https://cathalbyrnegit.github.io/datapond/reference/db_query.md)           | Both     | Run arbitrary SQL           |

### Interactive Tools

| Function                                                                            | Description                                         |
|-------------------------------------------------------------------------------------|-----------------------------------------------------|
| [`db_browser()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser.md) | Launch interactive Shiny browser for exploring data |

## Learn More

- [`vignette("concepts")`](https://cathalbyrnegit.github.io/datapond/articles/concepts.md) -
  Background on data lakes, hive partitioning, and DuckLake
- [`vignette("code-walkthrough")`](https://cathalbyrnegit.github.io/datapond/articles/code-walkthrough.md) -
  Detailed explanation of how the package works

## Contributing

Found a bug or have a feature request? Please open an issue on GitHub.
