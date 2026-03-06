# R/db_read.R

#' Read a DuckLake table (lazy)
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param version Optional integer snapshot version for time travel
#' @param timestamp Optional timestamp string for time travel (e.g. "2025-05-26 00:00:00")
#' @return A lazy tbl_duckdb object
#' @examples
#' \dontrun{
#' # Basic read
#' db_read(table = "imports")
#'
#' # From a specific schema
#' db_read(schema = "trade", table = "imports")
#'
#' # Time travel by version
#' db_read(table = "imports", version = 5)
#'
#' # Time travel by timestamp
#' db_read(table = "imports", timestamp = "2025-05-26 00:00:00")
#' }
#' @export
db_read <- function(schema = "main", table, version = NULL, timestamp = NULL) {
  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  if (!is.null(version) && !is.null(timestamp)) {
    stop("Use only one of 'version' or 'timestamp', not both.", call. = FALSE)
  }

  at_sql <- ""
  if (!is.null(version)) {
    at_sql <- glue::glue(" AT (VERSION => {as.integer(version)})")
  } else if (!is.null(timestamp)) {
    at_sql <- glue::glue(" AT (TIMESTAMP => {.db_sql_quote(timestamp)})")
  }

  from_sql <- glue::glue("{catalog}.{schema}.{table}{at_sql}")
  query <- glue::glue("SELECT * FROM {from_sql}")

  # Permission/existence check
  ok <- TRUE
  err <- NULL
  tryCatch({
    DBI::dbGetQuery(con, paste0(query, " LIMIT 1"))
  }, error = function(e) {
    ok <<- FALSE
    err <<- e$message
  })

  if (!ok) {
    stop(
      "Unable to read table '", catalog, ".", schema, ".", table, "'.\n",
      "Possible causes:\n",
      " - Table does not exist\n",
      " - You do not have access rights\n",
      " - Invalid version/timestamp for time travel\n\n",
      "DuckDB message: ", err,
      call. = FALSE
    )
  }

  dplyr::tbl(con, dplyr::sql(query))
}
