# Shiny module UI for db_browser()

Use `db_browser_ui()` and
[`db_browser_server()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_server.md)
to include the data lake browser as a Shiny module in your own app.

## Usage

``` r
db_browser_ui(id, height = "500px")
```

## Arguments

- id:

  Character of length 1, module ID

- height:

  Height of data preview tables (default "500px")

## Value

A Shiny UI element

## See also

Other shiny:
[`db_browser()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser.md),
[`db_browser_server()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_server.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# In your Shiny app UI:
ui <- fluidPage(
  db_browser_ui("browser1")
)

# In your Shiny app server:
server <- function(input, output, session) {
  db_browser_server("browser1")
}
} # }
```
