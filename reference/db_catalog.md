# List tables and file stats tracked by DuckLake

Returns information about all tables in the connected DuckLake catalog,
including row counts, file counts, and storage statistics.

## Usage

``` r
db_catalog()
```

## Value

A data.frame of table information with columns: schema_name, table_name,
file_count, total_rows, total_bytes, avg_file_bytes, avg_rows_per_file

## See also

[`db_tables()`](https://cathalbyrnegit.github.io/datapond/reference/db_tables.md)
to list just table names,
[`db_file_stats()`](https://cathalbyrnegit.github.io/datapond/reference/db_file_stats.md)
for detailed stats

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()
db_catalog()
} # }
```
