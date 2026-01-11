# Make a dataset discoverable in the public catalog (Hive mode only)

Makes metadata discoverable organisation-wide by copying it to the
shared `_catalog/` folder.

This function is only available in hive mode. In DuckLake mode, use
schema paths with folder ACLs to control access.

## Usage

``` r
db_set_public(section, dataset)
```

## Arguments

- section:

  Section name

- dataset:

  Dataset name

## Value

Invisibly returns TRUE

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect("//CSO-NAS/DataLake")
db_set_public(section = "Trade", dataset = "Imports")
} # }
```
