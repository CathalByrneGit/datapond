# datapond Examples

This directory contains example scripts demonstrating package features.

## Running Examples

```r
library(datapond)

# List available examples
run_example()

# Run a specific example
run_example("browser_demo_hive")
run_example("browser_demo_ducklake")
```

Or source directly:

```r
source(system.file("examples", "browser_demo_hive.R", package = "datapond"))
```

## Available Examples

### browser_demo_hive.R

Demonstrates `db_browser()` with hive-partitioned data:
- Creates sample datasets (Trade, Labour, Health, Reference)
- Adds documentation metadata
- Launches the interactive browser

### browser_demo_ducklake.R

Same as above but using DuckLake mode:
- Creates schemas and tables
- Shows snapshot history
- Demonstrates time-travel capable storage

## Also includes an entire workflow example
