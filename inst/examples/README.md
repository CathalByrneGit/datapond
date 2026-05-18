# datapond Examples

This directory contains example scripts demonstrating package features.

## Running Examples

```r
library(datapond)

# List available examples
run_example()

# Run a specific example
run_example("browser_demo_ducklake")
```

Or source directly:

```r
source(system.file("examples", "browser_demo_ducklake.R", package = "datapond"))
```

## Available Examples

### browser_demo_ducklake.R

Interactive demo of the data lake browser with sample DuckLake data:

- Creates a temporary DuckLake catalog (SQLite backend)
- Creates schemas: trade, labour, health, reference
- Populates sample tables with realistic data
- Adds documentation (descriptions, owners, tags, column metadata)
- Demonstrates automatic folder organization for access control
- Launches the interactive browser

**Features demonstrated:**

- `db_connect()` - Connect to DuckLake
- `db_create_schema()` - Create schemas
- `db_write()` - Write data with commit metadata
- `db_describe()` / `db_describe_column()` - Add documentation
- `db_browser()` - Launch interactive browser
- `db_snapshots()` - View snapshot history
- `db_status()` - Check connection info

### workflow.Rmd

A complete R Markdown workflow example showing end-to-end data lake usage.
