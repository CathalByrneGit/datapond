# Set sort order for a table

Configures the sort order for data files in a table. When new data is
written, it will be sorted by these columns within each file. This
improves query performance for range scans and filters on the sorted
columns.

## Usage

``` r
db_set_clustering(schema = "main", table, columns)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- columns:

  Character vector of column names to sort by, in order of priority. Use
  NULL to remove sort order.

## Value

Invisibly returns the qualified table name

## See also

[`db_recluster()`](https://cathalbyrnegit.github.io/datapond/reference/db_recluster.md)
to apply sort order to existing data,
[`db_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_write.md)
with `sort_by` parameter

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Set sort order for time-series queries
db_set_clustering(table = "events", columns = c("event_date", "user_id"))

# Remove sort order
db_set_clustering(table = "events", columns = NULL)
} # }
```
