# Shiny module server for db_browser()

Use
[`db_browser_ui()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_ui.md)
and `db_browser_server()` to include the data lake browser as a Shiny
module in your own app.

## Usage

``` r
db_browser_server(id, height = "500px")
```

## Arguments

- id:

  Character of length 1, module ID (must match UI)

- height:

  Height of data preview tables

## Value

A Shiny module server function

## See also

Other shiny:
[`db_browser()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser.md),
[`db_browser_ui()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_ui.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# In your Shiny app:
server <- function(input, output, session) {
  db_browser_server("browser1")
}
} # }
```
