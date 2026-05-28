# Search for tables

Search for tables by name, description, owner, or tags.

## Usage

``` r
db_search(pattern, field = c("all", "name", "description", "owner", "tags"))
```

## Arguments

- pattern:

  Search pattern (case-insensitive, matches partial strings)

- field:

  Field to search: "all" (default), "name", "description", "owner",
  "tags"

## Value

A data.frame of matching tables with their documentation

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Search everywhere
db_search("trade")

# Search only tags
db_search("official", field = "tags")

# Search by owner
db_search("Trade Section", field = "owner")
} # }
```
