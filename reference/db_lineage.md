# Record data lineage

Records the source(s) of a table for data lineage tracking. Lineage is
stored in the table's comment as JSON metadata.

## Usage

``` r
db_lineage(schema = "main", table, sources, transformation = NULL)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- sources:

  Character vector of source table names or descriptions

- transformation:

  Description of how data was transformed

## Value

Invisibly returns TRUE

## See also

[`db_get_lineage()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_lineage.md)
to retrieve lineage

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

db_lineage(
  table = "monthly_summary",
  sources = c("raw.transactions", "raw.products"),
  transformation = "Aggregated by month and product category"
)
} # }
```
