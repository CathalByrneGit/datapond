# Set partitioning for a DuckLake table

Configures partition keys for a DuckLake table. When partitioning is
set, new data written to the table will be split into separate files
based on the partition key values.

Partitioning enables:

- Efficient query pruning (only read relevant partitions)

- Potential folder-based access control at partition level

- Better data organization for time-series data

Note: Existing data is not reorganized - only new inserts are
partitioned.

## Usage

``` r
db_set_partitioning(schema = "main", table, partition_by)
```

## Arguments

- schema:

  Schema name

- table:

  Table name

- partition_by:

  Character vector of column names or expressions to partition by. Use
  NULL to remove partitioning.

## Value

Invisibly returns TRUE on success

## Examples

``` r
if (FALSE) { # \dontrun{
db_lake_connect(...)

# Partition by year and month columns
db_set_partitioning("trade", "imports", c("year", "month"))

# Partition using date functions
db_set_partitioning("trade", "imports", c("year(date)", "month(date)"))

# Remove partitioning (new data won't be partitioned)
db_set_partitioning("trade", "imports", NULL)
} # }
```
