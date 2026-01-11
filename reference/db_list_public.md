# List all datasets in the public catalog (Hive mode only)

Lists all entries published to the discovery catalog. This works even if
you don't have access to the underlying data, allowing organisation-wide
data discovery.

This function is only available in hive mode. In DuckLake mode, use
[`db_dictionary()`](https://cathalbyrnegit.github.io/datapond/reference/db_dictionary.md)
for data discovery, with access controlled via schema paths.

## Usage

``` r
db_list_public(section = NULL)
```

## Arguments

- section:

  Optional section to filter by

## Value

A data.frame with discovery information

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect("//CSO-NAS/DataLake")
db_list_public()
db_list_public(section = "Trade")
} # }
```
