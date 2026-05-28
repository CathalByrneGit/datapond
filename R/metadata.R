# R/metadata.R

#' List DuckLake snapshots
#'
#' @description Returns all snapshots (versions) for the connected DuckLake catalog,
#' including snapshot ID, timestamp, and commit metadata.
#' @return A data.frame of snapshot information
#' @examples
#' \dontrun{
#' db_connect()
#' db_snapshots()
#' }
#' @export
db_snapshots <- function() {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  sql <- glue::glue("FROM ducklake_snapshots({.db_sql_quote(catalog)})")
  DBI::dbGetQuery(con, sql)
}

#' List tables and file stats tracked by DuckLake
#'
#' @description Returns information about all tables in the connected DuckLake catalog,
#' including row counts, file counts, and storage statistics.
#' @return A data.frame of table information with columns: schema_name, table_name,
#'   file_count, total_rows, total_bytes, avg_file_bytes, avg_rows_per_file
#' @examples
#' \dontrun{
#' db_connect()
#' db_catalog()
#' }
#' @seealso [db_tables()] to list just table names, [db_file_stats()] for detailed stats
#' @export
db_catalog <- function() {
  # Delegate to db_file_stats which uses the documented ducklake_list_files API
  db_file_stats(schema = NULL, table = NULL)
}
