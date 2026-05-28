# Get lineage information

Retrieves lineage information for a table.

## Usage

``` r
db_get_lineage(schema = "main", table)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

## Value

A list with sources and transformation, or NULL if not recorded

## See also

[`db_lineage()`](https://cathalbyrnegit.github.io/datapond/reference/db_lineage.md)
to record lineage
