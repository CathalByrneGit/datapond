# Run arbitrary SQL and return results

Escape hatch for power users who need to run custom SQL queries.

## Usage

``` r
db_query(sql, collect = TRUE)
```

## Arguments

- sql:

  SQL query string

- collect:

  If TRUE (default), returns a collected data.frame. If FALSE, returns a
  lazy tbl reference.

## Value

Query results as data.frame or lazy tbl

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Run a custom query
db_query("SELECT * FROM main.products WHERE price > 100")

# Get a lazy reference
lazy_result <- db_query("SELECT * FROM main.products", collect = FALSE)
} # }
```
