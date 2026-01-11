# Check if a dataset is in the public catalog (Hive mode only)

Check whether metadata has been published to the discovery catalog.

This function is only available in hive mode. In DuckLake mode, access
control is managed via schema paths and folder ACLs.

## Usage

``` r
db_is_public(section, dataset)
```

## Arguments

- section:

  Section name

- dataset:

  Dataset name

## Value

Logical TRUE if public, FALSE otherwise

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect("//CSO-NAS/DataLake")
db_is_public(section = "Trade", dataset = "Imports")
} # }
```
