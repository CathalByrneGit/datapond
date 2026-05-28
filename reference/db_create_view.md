# Create a view in DuckLake

Creates a SQL view stored in the DuckLake catalog. Views support time
travel - attaching at a previous snapshot will reflect the view
definition that existed at that point in time.

## Usage

``` r
db_create_view(schema = "main", view, query, replace = FALSE)
```

## Arguments

- schema:

  Schema name (default "main")

- view:

  View name

- query:

  SQL query defining the view

- replace:

  If TRUE, replace existing view (default FALSE)

## Value

Invisibly returns the qualified view name

## See also

[`db_list_views()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_views.md),
[`db_drop_view()`](https://cathalbyrnegit.github.io/datapond/reference/db_drop_view.md)

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect(...)

# Create a simple view
db_create_view(view = "active_users",
               query = "SELECT * FROM users WHERE active = true")

# Replace existing view
db_create_view(view = "active_users",
               query = "SELECT * FROM users WHERE active = true AND verified = true",
               replace = TRUE)

# View with aggregation
db_create_view(schema = "reports", view = "monthly_totals",
               query = "SELECT year, month, SUM(value) as total FROM sales GROUP BY year, month")
} # }
```
