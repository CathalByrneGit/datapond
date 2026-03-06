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

| Term                | Meaning                                                                         |
|---------------------|---------------------------------------------------------------------------------|
| **Parquet**         | Columnar file format for storing tabular data efficiently                       |
| **DuckLake**        | Metadata layer that adds versioning and transactions to Parquet files           |
| **Catalog**         | Database storing DuckLake metadata (can be DuckDB, SQLite, or PostgreSQL)       |
| **Snapshot**        | A point-in-time version of the data in DuckLake                                 |
| **Time travel**     | Querying data as it existed at a past version or timestamp                      |
| **Schema**          | A namespace for organising related tables                                       |
| **Partition**       | Organising data by column values (e.g., year, month) for faster queries         |
| **ACID**            | Atomicity, Consistency, Isolation, Durability - database reliability guarantees |
| **Upsert**          | Update existing rows + insert new rows in one operation                         |
| **Data dictionary** | Documentation of all datasets, their columns, types, and descriptions           |

------------------------------------------------------------------------

## Next Steps

- See
  [`vignette("code-walkthrough")`](https://cathalbyrnegit.github.io/datapond/articles/code-walkthrough.md)
  for detailed explanation of how the package code works
- Try the examples in the README to get hands-on experience
- Use SQLite catalog on shared drives for team collaboration
