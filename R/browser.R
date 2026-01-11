# R/browser.R
# Shiny gadget for browsing the data lake

#' @title Run a package example script
#' @export
#' @description Runs one of the example scripts included with the package.
#' @param name Name of the example (without .R extension)
#' @return Runs the example script
#' @examples
#' \dontrun{
#' # List available examples
#' run_example()
#'
#' # Run specific example
#' run_example("browser_demo_hive")
#' run_example("browser_demo_ducklake")
#' }
run_example <- function(name = NULL) {
  examples_dir <- system.file("examples", package = "datapond")

  if (examples_dir == "") {
    stop("Examples directory not found. Is the package installed?", call. = FALSE)
  }

  available <- list.files(examples_dir, pattern = "\\.R$")
  available_names <- sub("\\.R$", "", available)

  if (is.null(name)) {
    cat("Available examples:\n")
    for (ex in available_names) {
      cat("  -", ex, "\n")
    }
    cat("\nRun with: run_example(\"name\")\n")
    return(invisible(available_names))
  }

  if (!name %in% available_names) {
    stop(
      "Example '", name, "' not found.\n",
      "Available: ", paste(available_names, collapse = ", "),
      call. = FALSE
    )
  }

  script_path <- file.path(examples_dir, paste0(name, ".R"))
  cat("Running example:", name, "\n\n")
  source(script_path, local = FALSE)
}


#' @title Browse the data lake interactively
#' @export
#' @family shiny
#' @description Launches a Shiny app to browse datasets, view metadata,
#'   search for data, and preview tables.
#' @param height Height of the data preview table (default "500px")
#' @param viewer Where to display: "dialog" (RStudio viewer), "browser", or "pane"
#' @return Opens the browser app. Returns NULL invisibly.
#' @examples
#' \dontrun{
#' # Connect first
#' db_connect(path = "//CSO-NAS/DataLake")
#'
#' # Launch browser
#' db_browser()
#'
#' # Or with DuckLake
#' db_lake_connect(...)
#' db_browser()
#' }
db_browser <- function(height = "500px",
                       viewer = c("dialog", "browser", "pane")) {

  .db_assert_browser_packages()

  viewer <- match.arg(viewer)

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  ui <- db_browser_app_ui(height = height)

  server <- function(input, output, session) {
    db_browser_server(id = "db_browser", height = height)
  }

  app <- shiny::shinyApp(ui = ui, server = server)

  # Choose viewer
  viewer_func <- switch(viewer,
    dialog = shiny::dialogViewer("datapond Browser", width = 1200, height = 800),
    browser = shiny::browserViewer(),
    pane = shiny::paneViewer()
  )

  shiny::runGadget(app, viewer = viewer_func, stopOnCancel = TRUE)

  invisible(NULL)
}


#' @title Full browser app UI
#' @keywords internal
#' @noRd
db_browser_app_ui <- function(height = "500px") {
  db_browser_ui(
    id = "db_browser",
    height = height
  )
}


#' Assert that required packages are available
#' @noRd
.db_assert_browser_packages <- function() {
  pkgs <- c("shiny", "bslib", "DT")
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]

  if (length(missing) > 0) {
    stop(
      "The following packages are required for db_browser(): ",
      paste(missing, collapse = ", "),
      "\nInstall with: install.packages(c(",
      paste0('"', missing, '"', collapse = ", "), "))",
      call. = FALSE
    )
  }

  invisible(TRUE)
}
