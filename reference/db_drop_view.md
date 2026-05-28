# Drop a view from DuckLake

Drop a view from DuckLake

## Usage

``` r
db_drop_view(schema = "main", view, if_exists = FALSE)
```

## Arguments

- schema:

  Schema name (default "main")

- view:

  View name

- if_exists:

  If TRUE, don't error if view doesn't exist (default FALSE)

## Value

Invisibly returns TRUE on success
