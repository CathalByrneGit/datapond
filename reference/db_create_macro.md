# Create a macro in DuckLake

Creates a SQL macro stored in the DuckLake catalog. Macros are reusable
SQL expressions that can be scalar (return a single value) or
table-valued (return a table). Macros support time travel.

## Usage

``` r
db_create_macro(
  schema = "main",
  name,
  params = character(0),
  body,
  table_macro = FALSE,
  replace = FALSE
)
```

## Arguments

- schema:

  Schema name (default "main")

- name:

  Macro name

- params:

  Character vector of parameter names, or named character vector with
  types (e.g., `c(a = "INTEGER", b = "VARCHAR")`)

- body:

  SQL expression for the macro body

- table_macro:

  If TRUE, create a table macro (returns a table). Default FALSE.

- replace:

  If TRUE, replace existing macro (default FALSE)

## Value

Invisibly returns the qualified macro name

## See also

[`db_list_macros()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_macros.md),
[`db_drop_macro()`](https://cathalbyrnegit.github.io/datapond/reference/db_drop_macro.md)

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect(...)

# Scalar macro
db_create_macro(name = "add_values",
                params = c("a", "b"),
                body = "a + b")

# Table macro
db_create_macro(name = "filtered_sales",
                params = c(min_value = "INTEGER"),
                body = "SELECT * FROM sales WHERE value > min_value",
                table_macro = TRUE)

# Use the macros
db_query("SELECT add_values(10, 20)")
db_query("SELECT * FROM filtered_sales(100)")
} # }
```
