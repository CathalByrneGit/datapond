# Re-cluster table data

Rewrites table data files to match the current clustering order. Use
this after setting clustering on a table that already contains data, or
after many appends have fragmented the sort order.

DuckLake automatically sorts data during compaction based on the table's
sort order (SET SORTED BY), so this function compacts files to re-sort.

## Usage

``` r
db_recluster(schema = "main", table, max_files = NULL)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- max_files:

  Maximum number of compaction operations per call. Lower values use
  less memory. Default NULL processes all files.

## Value

Invisibly returns the qualified table name

## See also

[`db_set_clustering()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_clustering.md)
to configure clustering order,
[`db_compact()`](https://cathalbyrnegit.github.io/datapond/reference/db_compact.md)

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Set clustering then recluster existing data
db_set_clustering(table = "events", columns = c("event_date"))
db_recluster(table = "events")

# Recluster with memory limit
db_recluster(table = "events", max_files = 100)
} # }
```
