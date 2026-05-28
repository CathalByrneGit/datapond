# Add comment/metadata to table or column

Adds metadata to a table or column using DuckLake's native COMMENT ON
statement. Comments are stored in the DuckLake catalog and support time
travel.

The comment can be a simple string or a list with structured metadata.
Lists are automatically converted to JSON for storage.

## Usage

``` r
db_comment(schema = "main", table, column = NULL, comment)
```

## Arguments

- schema:

  Schema name (default "main")

- table:

  Table name

- column:

  Optional column name. If NULL, comment is added to the table.

- comment:

  The comment - either a string or a list. Lists are converted to JSON.
  Use NULL to remove comment.

  For tables, common list fields: `description`, `owner`, `tags`,
  `lineage_sources`, `lineage_transformation`.

  For columns, common list fields: `description`, `units`, `notes`.

## Value

Invisibly returns TRUE on success

## See also

[`db_get_docs()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_docs.md)
to retrieve documentation

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect(...)

# Simple string comment
db_comment(table = "users", comment = "Active user accounts")

# Structured table metadata (stored as JSON)
db_comment(table = "imports", comment = list(
  description = "Monthly import values by country",
  owner = "Trade Section",
  tags = c("trade", "monthly", "official")
))

# Structured column metadata
db_comment(table = "imports", column = "value", comment = list(
  description = "Import value",
  units = "EUR (thousands)"
))

# Remove comment
db_comment(table = "users", comment = NULL)
} # }
```
