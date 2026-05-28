# Generate a data dictionary

Creates a data dictionary summarizing all tables with their
documentation, schemas, and column information.

## Usage

``` r
db_dictionary(schema = NULL, include_columns = TRUE)
```

## Arguments

- schema:

  Limit to specific schema (optional)

- include_columns:

  Include column-level details (default TRUE)

## Value

A data.frame with the data dictionary

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()
dict <- db_dictionary()

# Export to Excel
writexl::write_xlsx(dict, "data_dictionary.xlsx")
} # }
```
