# Browse the data lake interactively

Launches a Shiny app to browse datasets, view metadata, search for data,
and preview tables.

## Usage

``` r
db_browser(height = "500px", viewer = c("dialog", "browser", "pane"))
```

## Arguments

- height:

  Height of the data preview table (default "500px")

- viewer:

  Where to display: "dialog" (RStudio viewer), "browser", or "pane"

## Value

Opens the browser app. Returns NULL invisibly.

## See also

Other shiny:
[`db_browser_server()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_server.md),
[`db_browser_ui()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_ui.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Connect first
db_connect(path = "//CSO-NAS/DataLake")

# Launch browser
db_browser()

# Or with DuckLake
db_lake_connect(...)
db_browser()
} # }
```
