# Drop a macro from DuckLake

Drop a macro from DuckLake

## Usage

``` r
db_drop_macro(schema = "main", name, if_exists = FALSE)
```

## Arguments

- schema:

  Schema name (default "main")

- name:

  Macro name

- if_exists:

  If TRUE, don't error if macro doesn't exist (default FALSE)

## Value

Invisibly returns TRUE on success
