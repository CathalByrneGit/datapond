# Data Lake Concepts

This vignette explains the core concepts behind `datapond` - what data
lakes are, how DuckLake works, and how to use it effectively. No prior
knowledge assumed.

## The Problem We’re Solving

Within an organisation, different sections need to share data:

- **Trade** produces import/export figures that **National Accounts**
  needs
- **Labour** produces employment data that multiple sections consume
- **Health** produces statistics that feed into other analyses

Currently, this happens through shared network drives with folder-based
access control. It works, but has limitations:

- No versioning (if someone overwrites a file, the old version is gone)
- No way to know when data changed or who changed it
- Large files are slow to query (must read the whole thing)
- No schema enforcement (columns can change without warning)

`datapond` addresses these issues while keeping the familiar
folder-and-permissions model that IT already supports.

------------------------------------------------------------------------

## What is a Data Lake?

A **data lake** is a storage system designed to hold large amounts of
structured data in files (typically Parquet format) rather than in a
traditional database.

    Traditional Database          Data Lake
    ┌─────────────────┐          ┌─────────────────┐
    │  Database       │          │  Files on disk  │
    │  Server         │          │  (Parquet)      │
    │                 │          │                 │
    │  ┌───────────┐  │          │  📁 Trade/      │
    │  │ Table A   │  │          │    📄 data.parquet
    │  ├───────────┤  │          │  📁 Labour/     │
    │  │ Table B   │  │          │    📄 data.parquet
    │  └───────────┘  │          │                 │
    └─────────────────┘          └─────────────────┘
         ↓                              ↓
      Needs server               Just files!
      Needs DBA                  Query with DuckDB
      Licensing costs            Free & fast

**Why Parquet?**

Parquet is a columnar file format that’s:

- **Compressed** - files are 5-10x smaller than CSV
- **Fast** - only reads the columns you need
- **Typed** - preserves data types (dates, numbers, strings)
- **Universal** - works with R, Python, SAS, Excel, and more

------------------------------------------------------------------------

## What is DuckLake?

**DuckLake** is an extension for DuckDB that adds database-like features
on top of Parquet files. Think of it as “Parquet files with
superpowers”.

    Plain Parquet Files            DuckLake
    ┌─────────────────┐          ┌─────────────────┐
    │ Just files      │          │ Files +         │
    │                 │          │ Metadata catalog│
    │ 📄 data.parquet │          │                 │
    │                 │          │ 📄 catalog.sqlite
    │ No versioning   │          │ 📁 data/        │
    │ No transactions │          │   📄 file1.parquet
    │                 │          │   📄 file2.parquet
    └─────────────────┘          │                 │
                                 │ ✅ Versioning   │
                                 │ ✅ Transactions │
                                 │ ✅ Time travel  │
                                 │ ✅ Partitioning │
                                 └─────────────────┘

### The Metadata Catalog

DuckLake needs a place to store metadata about your tables. This
**catalog** tracks:

- Which Parquet files belong to which table
- The schema (columns and types) of each table
- A history of all changes (snapshots)
- Who made changes and when
- Partition keys for each table

The actual data is still in Parquet files - DuckLake just adds a
management layer.

------------------------------------------------------------------------

## Choosing a Catalog Backend

DuckLake supports three different backends for storing this catalog
metadata:

### 1. DuckDB (Single User)

``` r
db_connect(
  catalog_type = "duckdb",
  metadata_path = "metadata.ducklake",
  data_path = "//CSO-NAS/DataLake/data"
)
```

- Metadata stored in a `.ducklake` file
- **Single client only** - if two people connect, one will fail
- Good for: personal use, development, testing

### 2. SQLite (Multiple Local Users) - RECOMMENDED

``` r
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)
```

- Metadata stored in a `.sqlite` file on the network drive
- **Multiple readers + single writer** with automatic retry
- Still just a file - no server needed
- Good for: **shared network drives, most use cases**

**How SQLite handles concurrency:**

When someone is writing, other writers will wait and retry
automatically. Readers can continue uninterrupted. This works well for
typical usage where writes are less frequent than reads.

### 3. PostgreSQL (Multi-User Lakehouse)

``` r
db_connect(
  catalog_type = "postgres",
  metadata_path = "dbname=ducklake_catalog host=db.cso.ie",
  data_path = "//CSO-NAS/DataLake/data"
)
```

- Metadata stored in a PostgreSQL database
- **Full concurrent access** - multiple readers and writers
- Requires PostgreSQL 12+ server
- Good for: large teams, high write concurrency, remote access

### Which Should You Choose?

    Are you the only user?
      └─ Yes → DuckDB (simplest)
      └─ No → Are you on a shared network drive?
                └─ Yes → SQLite (recommended)
                └─ No → Do you need high write concurrency?
                          └─ Yes → PostgreSQL
                          └─ No → SQLite

**For most use cases, start with SQLite.** It’s still just a file
(familiar, works with IT permissions), but handles multiple users
gracefully.

------------------------------------------------------------------------

## Key DuckLake Features

### 1. Time Travel

Query data as it existed at any point in the past:

``` r
# Current data
products <- db_read(table = "products")

# Data as of version 5
products_v5 <- db_read(table = "products", version = 5)

# Data as of last Tuesday
products_tue <- db_read(table = "products",
                        timestamp = "2025-01-14 00:00:00")
```

This is invaluable when:

- Someone reports “the numbers looked different yesterday” - you can
  check!
- You need to reproduce an analysis from a specific date
- Something went wrong and you need to see the before/after

### 2. Snapshots and Audit Trail

Every change creates a new **snapshot** with metadata:

``` r
db_snapshots()
#>   snapshot_id snapshot_time       commit_author commit_message
#> 1           1 2025-01-01 09:00:00 jsmith        Initial load
#> 2           2 2025-01-15 14:30:00 mjones        Added Q4 data
#> 3           3 2025-01-20 11:00:00 mjones        Fixed country codes
```

You always know what changed, when, and (if recorded) why.

### 3. ACID Transactions

Changes are **atomic** - they either fully succeed or fully fail. No
partial updates that leave data in a broken state.

``` r
# If this fails halfway through, no data is changed
db_write(big_dataset, table = "imports", mode = "overwrite")
```

### 4. Schema Evolution

DuckLake handles schema changes gracefully:

- Add a new column? Old data gets NULLs for that column
- Query old versions? They still work with the old schema

### 5. Hive Partitioning

DuckLake supports partitioning tables by column values for improved
query performance:

``` r
# Create a partitioned table
db_write(
  my_data,
  table = "imports",
  partition_by = c("year", "month")
)

# Check partition keys
db_get_partitioning(table = "imports")
#> [1] "year"  "month"

# Queries filtering by partition columns are much faster
imports <- db_read(table = "imports")
imports |> filter(year == 2024, month == 1) |> collect()
```

### 6. Bucket Partitioning (DuckLake 1.0+)

For high-cardinality columns (like user_id, order_id), bucket
partitioning distributes data into a fixed number of files using
Iceberg-compatible Murmur3 hashing:

``` r
# Bucket partition by user_id into 16 buckets
db_write(
  events_data,
  table = "events",
  bucket_by = list(column = "user_id", buckets = 16)
)

# Combine hive and bucket partitioning
db_write(
  events_data,
  table = "events",
  partition_by = "year",
  bucket_by = list(column = "user_id", buckets = 8)
)
```

**When to use bucket partitioning:** - High-cardinality columns
(millions of distinct values) - Hive partitioning would create too many
small files - You need efficient point lookups by the partitioned column

### 7. Clustering / Sorted Tables (DuckLake 1.0+)

Clustering keeps data sorted within files for faster range scans:

``` r
# Create a clustered table
db_write(
  sales_data,
  table = "sales",
  sort_by = c("sale_date", "region")
)

# Set clustering on an existing table
db_set_clustering(table = "events", columns = c("event_date", "user_id"))

# Re-sort existing data to match clustering order
db_recluster(table = "events")
```

Clustering improves performance for: - Time-series queries
(`WHERE event_date BETWEEN ...`) - Range scans on clustered columns -
Queries that filter on multiple clustered columns

### 8. Data Inlining (DuckLake 1.0+)

Data inlining solves the “small file problem” for streaming/frequent
writes by staging data in the catalog database instead of creating new
parquet files:

``` r
# Stream small batches with inlining
for (batch in batches) {
  db_write(batch, table = "events", mode = "append", inline = TRUE)
}

# Flush inlined data to parquet when ready
db_flush_inlined(table = "events")

# Configure auto-flush threshold (rows before automatic flush)
db_set_inline_threshold(table = "events", threshold = 50000)
```

**When to use inlining:** - Frequent small writes (streaming data,
real-time updates) - IoT or event data arriving in small batches -
Avoiding many small parquet files

### 9. Iceberg Compatibility (DuckLake 1.0+)

Export DuckLake tables to Iceberg format for use with other engines
(Spark, Trino, Presto):

``` r
# Export to Iceberg format
db_export_iceberg(table = "sales")

# Export with specific catalog type
db_export_iceberg(table = "sales", catalog_type = "hive")

# Get Iceberg-compatible metadata
meta <- db_iceberg_metadata(table = "sales")
meta$schema
meta$partition_spec
```

------------------------------------------------------------------------

## Schemas: Organising Tables

In DuckLake, **schemas** are like folders for tables. They help organise
related tables together.

    catalog (datapond)
    ├── main (default schema)
    │   └── reference_tables
    ├── trade
    │   ├── imports
    │   ├── exports
    │   └── products
    ├── labour
    │   ├── employment
    │   └── earnings
    └── health
        ├── hospitals
        └── waiting_times

### Using Schemas

``` r
# Create a schema for your section
db_create_schema("trade")

# Write to it
db_write(imports_data, schema = "trade", table = "imports")

# Read from it
imports <- db_read(schema = "trade", table = "imports")

# List tables in a schema
db_tables("trade")
```

------------------------------------------------------------------------

## Data Documentation

Good data governance requires documentation. `datapond` provides
built-in tools to document your datasets and generate a data dictionary.

### Documenting Tables

``` r
# Add metadata to a table
db_describe(
  table = "imports",
  description = "Monthly import values by country and HS commodity code",
  owner = "Trade Section",
  tags = c("trade", "monthly", "official")
)

# Document individual columns
db_describe_column(
  table = "imports",
  column = "value",
  description = "Import value",
  units = "EUR (thousands)"
)

db_describe_column(
  table = "imports",
  column = "country_code",
  description = "ISO 3166-1 alpha-2 country code"
)
```

### Searching and Discovery

``` r
# Search tables by any field
db_search("trade")                        # Matches name, description, owner, or tags
db_search("monthly", field = "tags")      # Search only tags
db_search("Trade Section", field = "owner")  # Find tables by owner

# Find columns across all tables
db_search_columns("country")
#>   schema  table    column_name  column_type  column_description
#> 1 trade   imports  country_code VARCHAR      ISO 3166-1 alpha-2...
#> 2 trade   exports  country_code VARCHAR      ISO 3166-1 alpha-2...
#> 3 labour  survey   country      VARCHAR      Country of residence
```

### Generating a Data Dictionary

``` r
# Full data dictionary with column details
dict <- db_dictionary()

# Just table-level summary
dict_summary <- db_dictionary(include_columns = FALSE)

# Filter to specific schema
dict_trade <- db_dictionary(schema = "trade")

# Export to Excel
writexl::write_xlsx(dict, "data_dictionary.xlsx")
```

The data dictionary includes: - Table name and location - Description,
owner, tags - Column names, types, and documentation - Last updated
timestamps

------------------------------------------------------------------------

## Data Lineage

Track where your data comes from and how it was transformed:

``` r
# Record lineage when creating derived tables
db_lineage(
  table = "monthly_summary",
  sources = c("raw.transactions", "raw.products"),
  transformation = "Aggregated by month and product category"
)

# Later, retrieve lineage information
db_get_lineage(table = "monthly_summary")
#> $sources
#> [1] "raw.transactions" "raw.products"
#>
#> $transformation
#> [1] "Aggregated by month and product category"
#>
#> $recorded_at
#> [1] "2025-03-10 14:30:00"
```

Lineage information is stored in the `_metadata.lineage` table within
your DuckLake catalog.

------------------------------------------------------------------------

## Preview Before Writing

Before making changes to production data, you can preview what will
happen:

### Write Preview

``` r
db_preview_write(my_data, table = "imports", mode = "overwrite")
```

Shows: - Current vs new row counts - Schema comparison - Warnings (e.g.,
append to non-existent table)

### Upsert Preview

``` r
db_preview_upsert(my_data, table = "products", by = "product_id")
```

Shows: - How many rows will be **inserted** (new keys) - How many rows
will be **updated** (existing keys) - Warnings about duplicate keys in
incoming data

------------------------------------------------------------------------

## Access Control

DuckLake uses **file system permissions** - the same model commonly
used.

### Architecture

    //CSO-NAS/DataLake/
    ├── catalog.sqlite           ← Single DuckLake catalog
    └── data/                    ← Data organised automatically by schema/table
        ├── trade/               ← Trade team has access (folder ACLs)
        │   ├── imports/
        │   │   └── ducklake-uuid.parquet
        │   └── exports/
        │       └── ducklake-uuid.parquet
        ├── labour/              ← Labour team has access (folder ACLs)
        │   └── employment/
        │       └── ducklake-uuid.parquet
        └── reference/           ← Everyone has read access
            └── countries/
                └── ducklake-uuid.parquet

### Setting Up Schemas

``` r
# Connect to DuckLake
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# Create schemas - DuckLake creates folders automatically
db_create_schema("trade")
db_create_schema("labour")
db_create_schema("reference")

# Write data - files go to data/{schema}/{table}/ automatically
db_write(imports_data, schema = "trade", table = "imports")
db_write(countries, schema = "reference", table = "countries")

# Set folder ACLs on the schema folders:
# - //CSO-NAS/DataLake/data/trade/     → Trade team read/write
# - //CSO-NAS/DataLake/data/labour/    → Labour team read/write
# - //CSO-NAS/DataLake/data/reference/ → Everyone read
```

### How It Works

| Component                | Access Controlled By                                     |
|--------------------------|----------------------------------------------------------|
| Catalog file (`.sqlite`) | File permissions - need read to query, write to modify   |
| Schema data folder       | Folder ACLs - each schema can have different permissions |
| Table data               | Inherited from schema folder                             |

**Benefits**: - **Zero configuration** - folder structure created
automatically - **Familiar model** - uses standard folder permissions -
**Granular control** - different teams can own different schemas -
**Single catalog** - one metadata file, simpler management -
**IT-friendly** - works with existing permission infrastructure

------------------------------------------------------------------------

## File Maintenance

Over time, frequent small writes create many small Parquet files. This
can slow down queries. DuckLake provides tools to maintain optimal file
sizes.

### Checking File Statistics

``` r
# See file counts and sizes for all tables
db_file_stats()
#>   schema_name table_name file_count total_rows total_bytes avg_file_bytes
#> 1 trade       imports         523    1500000   125000000         239007
#> 2 trade       exports          12     500000    45000000        3750000

# Tables with many small files (< 10 MB average) are candidates for compaction
stats <- db_file_stats()
stats[stats$file_count > 100 & stats$avg_file_bytes < 1e7, ]
```

### Compacting Files

``` r
# Merge small files into larger ones
db_compact(table = "imports")
#> Compacting files...
#>   Table: imports
#> Compaction complete:
#>   Files before: 523
#>   Files after:  15
#>   Files merged: 508

# Compact with memory limits (for very large tables)
db_compact(table = "imports", max_files = 500)

# Compact an entire schema
db_compact(schema = "trade")
```

### Cleaning Up Old Files

After compacting or vacuuming, old files become orphaned. Clean them up
to reclaim disk space:

``` r
# Preview what would be deleted
db_cleanup_files(dry_run = TRUE)

# Actually remove orphaned files
db_cleanup_files(dry_run = FALSE)
```

### Recommended Maintenance Schedule

| Operation                                                                         | Frequency                   | Purpose                    |
|-----------------------------------------------------------------------------------|-----------------------------|----------------------------|
| `db_file_stats()`                                                                 | Weekly                      | Monitor file fragmentation |
| `db_compact()`                                                                    | Monthly or after bulk loads | Merge small files          |
| [`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md) | Monthly                     | Remove old snapshots       |
| `db_cleanup_files()`                                                              | After vacuum or compact     | Reclaim disk space         |

------------------------------------------------------------------------

## Putting It Together

Here’s how a typical workflow might look:

### Publishing Data (Producer)

``` r
library(datapond)

# Connect to DuckLake
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# Prepare your data
imports_q1 <- prepare_imports_data(raw_files)

# Preview what will happen
db_preview_write(imports_q1, schema = "trade", table = "imports", mode = "append")

# Publish with a meaningful commit message
db_write(
  imports_q1,
  schema = "trade",
  table = "imports",
  mode = "append",
  commit_author = Sys.info()["user"],
  commit_message = "Q1 2025 imports data - final"
)

# Document for discovery
db_describe(
  schema = "trade",
  table = "imports",
  description = "Monthly import values by country and HS code",
  owner = "Trade Section"
)

db_disconnect()
```

### Consuming Data (Consumer)

``` r
library(datapond)

# Connect to DuckLake (read access to catalog and data folder)
db_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/data"
)

# Discover what's available
db_list_schemas()
db_tables("trade")

# Search for relevant data
db_search("imports")

# Check documentation
db_get_docs(schema = "trade", table = "imports")

# Read and analyse
imports <- db_read(schema = "trade", table = "imports")

imports |>
  filter(year == 2025, quarter == 1) |>
  summarise(total_value = sum(value)) |>
  collect()

db_disconnect()
```

------------------------------------------------------------------------

## Glossary

| Term                 | Meaning                                                                               |
|----------------------|---------------------------------------------------------------------------------------|
| **Parquet**          | Columnar file format for storing tabular data efficiently                             |
| **DuckLake**         | Metadata layer that adds versioning and transactions to Parquet files                 |
| **Catalog**          | Database storing DuckLake metadata (can be DuckDB, SQLite, or PostgreSQL)             |
| **Snapshot**         | A point-in-time version of the data in DuckLake                                       |
| **Time travel**      | Querying data as it existed at a past version or timestamp                            |
| **Schema**           | A namespace for organising related tables                                             |
| **Hive Partition**   | Organising data by column values (e.g., year, month) into separate directories        |
| **Bucket Partition** | Distributing data into fixed buckets using hash function for high-cardinality columns |
| **Clustering**       | Keeping data sorted within files for faster range scans                               |
| **Inlining**         | Staging small writes in the catalog database to avoid creating many small files       |
| **ACID**             | Atomicity, Consistency, Isolation, Durability - database reliability guarantees       |
| **Upsert**           | Update existing rows + insert new rows in one operation                               |
| **Data dictionary**  | Documentation of all datasets, their columns, types, and descriptions                 |
| **Lineage**          | Tracking the sources and transformations that produced a dataset                      |
| **Compaction**       | Merging many small files into fewer larger files for better performance               |
| **Iceberg**          | Open table format for large analytic tables, compatible with Spark/Trino/Presto       |

------------------------------------------------------------------------

## Next Steps

- See
  [`vignette("code-walkthrough")`](https://cathalbyrnegit.github.io/datapond/articles/code-walkthrough.md)
  for detailed explanation of how the package code works
- Try the examples in the README to get hands-on experience
- Use SQLite catalog on shared drives for team collaboration
