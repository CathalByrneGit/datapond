# Read a DuckLake table as an Arrow Table

Reads a DuckLake table directly as an Arrow Table, bypassing DuckDB's
query engine. This is useful for interoperability with other Arrow-based
tools or when you need the raw Arrow format.

## Usage

``` r
db_read_arrow(schema = "main", table, columns = NULL, as_data_frame = TRUE)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- columns:

  Optional character vector of column names to read. If NULL (default),
  reads all columns.

- as_data_frame:

  If TRUE (default), converts to data.frame. If FALSE, returns an Arrow
  Table.

## Value

A data.frame or Arrow Table

## See also

[`db_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_read.md)
for lazy dplyr-based reading,
[`db_write_arrow()`](https://cathalbyrnegit.github.io/datapond/reference/db_write_arrow.md)
for writing Arrow data

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect(...)

# Read as data.frame (default)
df <- db_read_arrow(table = "imports")

# Read as Arrow Table
arrow_tbl <- db_read_arrow(table = "imports", as_data_frame = FALSE)

# Read specific columns
df <- db_read_arrow(table = "imports", columns = c("year", "value"))
} # }
```
