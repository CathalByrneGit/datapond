# R/browser_ui.R
# Shiny module UI for data lake browser

#' @title Shiny module UI for db_browser()
#' @export
#' @family shiny
#' @description Use `db_browser_ui()` and [db_browser_server()]
#'   to include the data lake browser as a Shiny module in your own app.
#' @param id Character of length 1, module ID
#' @param height Height of data preview tables (default "500px")
#' @return A Shiny UI element
#' @examples
#' \dontrun{
#' # In your Shiny app UI:
#' ui <- fluidPage(
#'   db_browser_ui("browser1")
#' )
#'
#' # In your Shiny app server:
#' server <- function(input, output, session) {
#'   db_browser_server("browser1")
#' }
#' }
db_browser_ui <- function(id, height = "500px") {

  ns <- shiny::NS(id)

  # Get current mode for conditional UI
  curr_mode <- .db_get("mode")
  is_hive <- identical(curr_mode, "hive")

  # Sidebar with tree view
  sidebar_content <- shiny::tagList(
    shiny::tags$h5(
      if (is_hive) "Sections & Datasets" else "Schemas & Tables",
      style = "margin-bottom: 10px; font-weight: bold;"
    ),
    shiny::uiOutput(ns("tree_view")),
    shiny::hr(),
    shiny::actionButton(ns("refresh_tree"), "Refresh",
                        icon = shiny::icon("sync"),
                        class = "btn-sm btn-outline-secondary")
  )

  sidebar <- bslib::sidebar(
    title = "Browse",
    id = ns("sidebar"),
    width = 280,
    sidebar_content
  )

  # Main content with tabs
  main_content <- bslib::navset_card_tab(
    id = ns("main_tabs"),

    # Preview tab
    bslib::nav_panel(
      title = "Preview",
      icon = shiny::icon("table"),
      shiny::div(
        style = "padding: 10px;",
        shiny::uiOutput(ns("selected_info")),
        shiny::hr(),
        shiny::fluidRow(
          shiny::column(4,
            shiny::numericInput(ns("preview_rows"), "Rows to show:",
                                value = 100, min = 10, max = 10000, step = 10)
          ),
          shiny::column(4,
            shiny::actionButton(ns("load_preview"), "Load Preview",
                                icon = shiny::icon("eye"),
                                class = "btn-primary",
                                style = "margin-top: 25px;")
          )
        ),
        shiny::hr(),
        DT::dataTableOutput(ns("preview_table"), height = height)
      )
    ),

    # Metadata tab
    bslib::nav_panel(
      title = "Metadata",
      icon = shiny::icon("info-circle"),
      shiny::div(
        style = "padding: 10px;",
        shiny::uiOutput(ns("metadata_display"))
      )
    ),

    # Search tab
    bslib::nav_panel(
      title = "Search",
      icon = shiny::icon("search"),
      shiny::div(
        style = "padding: 10px;",
        shiny::fluidRow(
          shiny::column(6,
            shiny::textInput(ns("search_pattern"), "Search pattern:",
                             placeholder = "Enter search term...")
          ),
          shiny::column(3,
            shiny::selectInput(ns("search_field"), "Search in:",
                               choices = c("All fields" = "all",
                                           "Name" = "name",
                                           "Description" = "description",
                                           "Owner" = "owner",
                                           "Tags" = "tags"))
          ),
          shiny::column(3,
            shiny::actionButton(ns("do_search"), "Search",
                                icon = shiny::icon("search"),
                                class = "btn-primary",
                                style = "margin-top: 25px;")
          )
        ),
        shiny::hr(),
        shiny::h5("Results"),
        DT::dataTableOutput(ns("search_results"), height = "400px"),
        shiny::hr(),
        shiny::h5("Search Columns"),
        shiny::fluidRow(
          shiny::column(6,
            shiny::textInput(ns("col_search_pattern"), "Column name pattern:",
                             placeholder = "e.g., country, _id")
          ),
          shiny::column(3,
            shiny::actionButton(ns("do_col_search"), "Search Columns",
                                icon = shiny::icon("columns"),
                                class = "btn-secondary",
                                style = "margin-top: 25px;")
          )
        ),
        DT::dataTableOutput(ns("col_search_results"), height = "300px")
      )
    ),

    # Dictionary tab
    bslib::nav_panel(
      title = "Dictionary",
      icon = shiny::icon("book"),
      shiny::div(
        style = "padding: 10px;",
        shiny::fluidRow(
          shiny::column(3,
            shiny::checkboxInput(ns("dict_include_cols"), "Include columns", value = FALSE)
          ),
          shiny::column(3,
            shiny::actionButton(ns("gen_dict"), "Generate Dictionary",
                                icon = shiny::icon("book"),
                                class = "btn-primary")
          ),
          shiny::column(3,
            shiny::downloadButton(ns("download_dict"), "Download CSV",
                                  class = "btn-outline-secondary")
          )
        ),
        shiny::hr(),
        DT::dataTableOutput(ns("dictionary_table"), height = height)
      )
    ),

    # Info tab
    bslib::nav_panel(
      title = "Connection",
      icon = shiny::icon("plug"),
      shiny::div(
        style = "padding: 10px;",
        shiny::verbatimTextOutput(ns("connection_info"))
      )
    )
  )

  # Combine into page
  bslib::page_sidebar(
    title = "datapond Browser",
    sidebar = sidebar,
    main_content,
    theme = bslib::bs_theme(version = 5, bootswatch = "flatly")
  )
}
