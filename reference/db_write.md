# Write a DuckLake table (overwrite/append)

Writes data to a DuckLake table. Supports two input types:

**data.frame/tibble**: Data is transferred from R to DuckDB. Use this
when you have data in R memory (e.g., from CSV, API, or computation).

**Lazy dbplyr table**: Data stays in DuckDB - no R memory used. Use this
for transformations within the lake (e.g., cleaning, aggregating). The
dplyr pipeline is converted to SQL and executed as
`CREATE TABLE AS SELECT` or `INSERT INTO ... SELECT`.

## Usage

``` r
db_write(
  data,
  schema = "main",
  table,
  mode = c("overwrite", "append"),
  col_types = NULL,
  partition_by = NULL,
  bucket_by = NULL,
  sort_by = NULL,
  inline = FALSE,
  commit_author = NULL,
  commit_message = NULL
)
```

## Arguments

- data:

  A data.frame, tibble, or lazy dbplyr table (from
  [`db_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_read.md)
  or `tbl()`). Lazy tables enable zero-copy transformations within
  DuckDB.

- schema:

  Schema name (default "main")

- table:

  Table name

- mode:

  "overwrite" or "append"

- col_types:

  Optional named list or character vector specifying column types. Only
  applies to data.frame input (ignored for lazy tables). Overrides
  automatic type inference for stricter schema control. Format:
  `list(id = "BIGINT", value = "DECIMAL(10,2)")` or
  `c(id = "BIGINT", value = "DECIMAL(10,2)")`. Supported types: INTEGER,
  BIGINT, DOUBLE, DECIMAL(p,s), VARCHAR, BOOLEAN, DATE, TIMESTAMP,
  INTERVAL, BLOB, GEOMETRY, etc.

- partition_by:

  Optional character vector of column names to partition by. Only valid
  for mode = "overwrite". On overwrite, if not specified, existing
  partitioning is preserved.

- bucket_by:

  Optional list specifying bucket partitioning for high-cardinality
  columns. Format: `list(column = "col_name", buckets = 16)`. Uses
  Iceberg-compatible Murmur3 hashing. Only valid for mode = "overwrite".

- sort_by:

  Optional character vector of column names to sort/cluster by. Improves
  query performance for range scans and filters on these columns. Only
  valid for mode = "overwrite".

- inline:

  Deprecated. DuckLake automatically inlines small writes based on the
  `data_inlining_row_limit` threshold (default 10 rows). Use
  [`db_set_inline_threshold()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_inline_threshold.md)
  to adjust the threshold for a table.

- commit_author:

  Optional author for DuckLake commit metadata

- commit_message:

  Optional message for DuckLake commit metadata

## Value

Invisibly returns the qualified table name

## See also

[`db_flush_inlined()`](https://cathalbyrnegit.github.io/datapond/reference/db_flush_inlined.md)
to flush inlined data,
[`db_set_clustering()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_clustering.md)
to change clustering on existing tables,
[`db_recluster()`](https://cathalbyrnegit.github.io/datapond/reference/db_recluster.md)
to re-sort data

## Examples

``` r
if (FALSE) { # \dontrun{
# ==== data.frame approach (data passes through R) ====

# Basic overwrite
db_write(my_data, table = "imports")

# With schema
db_write(my_data, schema = "trade", table = "imports")

# With partitioning (overwrite mode only)
db_write(my_data, schema = "trade", table = "imports",
         partition_by = c("year", "month"))

# With explicit column types for stricter schema control
db_write(my_data, table = "financials",
         col_types = list(id = "BIGINT", amount = "DECIMAL(12,2)"))

# ==== Lazy table approach (zero-copy, stays in DuckDB) ====

# Transform and write without collect() - no R memory used
db_read(table = "raw_imports") |>
  filter(year == 2024) |>
  mutate(value_eur = value * exchange_rate) |>
  group_by(country, month) |>
  summarise(total = sum(value_eur), .groups = "drop") |>
  db_write(schema = "clean", table = "monthly_summary")

# Append transformed data
db_read(table = "staging") |>
  filter(!is.na(id)) |>
  db_write(table = "production", mode = "append")

# Join tables and write result
orders <- db_read(table = "orders")
products <- db_read(table = "products")

orders |>
  left_join(products, by = "product_id") |>
  select(order_id, product_name, quantity, price) |>
  db_write(table = "order_details")

# ==== Other options ====

# DuckLake automatically inlines small writes (< threshold rows)
# Use db_set_inline_threshold() to adjust the threshold
db_write(batch, table = "events", mode = "append")

# With commit metadata
db_write(my_data, table = "imports", mode = "append",
         commit_author = "jsmith",
         commit_message = "Added Q3 data")
} # }
```
