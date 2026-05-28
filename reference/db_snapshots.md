# List DuckLake snapshots

Returns all snapshots (versions) for the connected DuckLake catalog,
including snapshot ID, timestamp, and commit metadata.

## Usage

``` r
db_snapshots()
```

## Value

A data.frame of snapshot information

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()
db_snapshots()
} # }
```
