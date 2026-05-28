# Get documentation for a table

Retrieve documentation metadata for a table and its columns. Metadata is
stored using native SQL COMMENT ON statements.

## Usage

``` r
db_get_docs(schema = "main", table)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

## Value

A list containing description, owner, tags, lineage, and column
documentation

## See also

[`db_comment()`](https://cathalbyrnegit.github.io/datapond/reference/db_comment.md)
to add documentation

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# First, add documentation
db_comment(table = "imports", comment = list(
  description = "Monthly import values",
  owner = "Trade Section",
  tags = c("trade", "monthly")
))

# Then retrieve it
db_get_docs(table = "imports")
} # }
```
