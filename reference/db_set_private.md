# Remove a dataset from the public catalog (Hive mode only)

Removes metadata from the public discovery catalog. The dataset and its
data remain unchanged.

This function is only available in hive mode. In DuckLake mode, use
schema paths with folder ACLs to control access.

## Usage

``` r
db_set_private(section, dataset)
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
db_set_private(section = "Trade", dataset = "Imports")
} # }
```
