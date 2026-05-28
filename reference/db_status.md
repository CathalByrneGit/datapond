# Get connection status and configuration

Returns information about the current connection state, including paths,
catalog configuration, and connection validity.

## Usage

``` r
db_status(verbose = TRUE)
```

## Arguments

- verbose:

  If TRUE, prints a formatted summary. If FALSE, returns a list
  silently.

## Value

A list (invisibly if verbose=TRUE) containing connection details.

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()
db_status()
} # }
```
