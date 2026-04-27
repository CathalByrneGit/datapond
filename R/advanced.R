# R/advanced.R
# Advanced DuckLake features: inlining, clustering, and Iceberg export

# ==============================================================================
# Data Inlining
# ==============================================================================

#' Flush inlined data to parquet files
#'
#' @description Writes data that was staged in the catalog database (via
#' `db_write(..., inline = TRUE)`) to parquet files. This is useful after
#' accumulating many small writes to consolidate them into proper data files.
#'
#' @param schema Schema name (default "main"). Use NULL for all schemas.
#' @param table Table name. Use NULL for all tables in the schema.
#' @return Invisibly returns TRUE on success
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Stream data with inlining
#' for (batch in batches) {
#'   db_write(batch, table = "events", mode = "append", inline = TRUE)
#' }
#'
#' # Flush inlined data to parquet
#' db_flush_inlined(table = "events")
#'
#' # Flush all inlined data in catalog
#' db_flush_inlined()
#' }
#' @seealso [db_write()] with `inline = TRUE`, [db_set_inline_threshold()]
#' @export
db_flush_inlined <- function(schema = "main", table = NULL) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  args <- c(.db_sql_quote(catalog))

  if (!is.null(table)) {
    table <- .db_validate_name(table, "table")
    args <- c(args, .db_sql_quote(table))
  }

  if (!is.null(schema)) {
    schema <- .db_validate_name(schema, "schema")
    args <- c(args, glue::glue("schema => {.db_sql_quote(schema)}"))
  }

  sql <- glue::glue("CALL ducklake_flush_inlined_data({paste(args, collapse = ', ')})")

  tryCatch({
    DBI::dbExecute(con, sql)
    message("Flushed inlined data to parquet files.")
  }, error = function(e) {
    if (grepl("no inlined data", e$message, ignore.case = TRUE)) {
      message("No inlined data to flush.")
    } else {
      stop("Flush failed: ", e$message, call. = FALSE)
    }
  })

  invisible(TRUE)
}


#' Set the inline threshold for a table
#'
#' @description Configures the row count threshold at which DuckLake automatically
#' flushes inlined data to parquet files. When the number of inlined rows exceeds
#' this threshold, they are automatically written to a parquet file.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param threshold Number of rows to accumulate before auto-flushing.
#'   Use 0 to disable inlining entirely. Default is typically 10000.
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Set threshold to 50000 rows before auto-flush
#' db_set_inline_threshold(table = "events", threshold = 50000)
#'
#' # Disable inlining (all writes go directly to parquet)
#' db_set_inline_threshold(table = "events", threshold = 0)
#' }
#' @seealso [db_write()] with `inline = TRUE`, [db_flush_inlined()]
#' @export
db_set_inline_threshold <- function(schema = "main", table, threshold = 10000) {
  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")

  if (!is.numeric(threshold) || threshold < 0) {
    stop("threshold must be a non-negative number.", call. = FALSE)
  }

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", table)

  sql <- glue::glue("ALTER TABLE {qname} SET (inline_row_threshold = {as.integer(threshold)})")
  DBI::dbExecute(con, sql)

  message("Set inline threshold for ", qname, " to ", threshold, " rows.")
  invisible(qname)
}


# ==============================================================================
# Clustering / Sorted Tables
# ==============================================================================

#' Set clustering order for a table
#'
#' @description Configures the sort order for data files in a table. When new
#' data is written, it will be sorted by these columns within each file.
#' This improves query performance for range scans and filters on the
#' clustering columns.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param columns Character vector of column names to cluster by, in order of priority.
#'   Use NULL to remove clustering.
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Set clustering for time-series queries
#' db_set_clustering(table = "events", columns = c("event_date", "user_id"))
#'
#' # Remove clustering
#' db_set_clustering(table = "events", columns = NULL)
#' }
#' @seealso [db_recluster()] to apply clustering to existing data, [db_write()]
#'   with `sort_by` parameter
#' @export
db_set_clustering <- function(schema = "main", table, columns) {
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

  qname <- paste0(catalog, ".", schema, ".", table)

  if (is.null(columns) || length(columns) == 0) {
    sql <- glue::glue("ALTER TABLE {qname} RESET CLUSTERING ORDER")
    DBI::dbExecute(con, sql)
    message("Removed clustering from ", qname)
  } else {
    if (!is.character(columns)) {
      stop("columns must be a character vector.", call. = FALSE)
    }
    sort_clause <- paste(columns, collapse = ", ")
    sql <- glue::glue("ALTER TABLE {qname} SET CLUSTERING ORDER BY ({sort_clause})")
    DBI::dbExecute(con, sql)
    message("Set clustering for ", qname, ": ", sort_clause)
  }

  invisible(qname)
}


#' Re-cluster table data
#'
#' @description Rewrites table data files to match the current clustering order.
#' Use this after setting clustering on a table that already contains data,
#' or after many appends have fragmented the sort order.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param max_files Maximum number of files to process in one operation.
#'   Lower values use less memory. Default NULL processes all files.
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Set clustering then recluster existing data
#' db_set_clustering(table = "events", columns = c("event_date"))
#' db_recluster(table = "events")
#'
#' # Recluster with memory limit
#' db_recluster(table = "events", max_files = 100)
#' }
#' @seealso [db_set_clustering()] to configure clustering order
#' @export
db_recluster <- function(schema = "main", table, max_files = NULL) {
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

  qname <- paste0(catalog, ".", schema, ".", table)

  args <- c(.db_sql_quote(catalog), .db_sql_quote(table))

  named_args <- c(glue::glue("schema => {.db_sql_quote(schema)}"))
  if (!is.null(max_files)) {
    if (!is.numeric(max_files) || max_files < 1) {
      stop("max_files must be a positive integer.", call. = FALSE)
    }
    named_args <- c(named_args, glue::glue("max_files => {as.integer(max_files)}"))
  }

  all_args <- c(args, named_args)
  sql <- glue::glue("CALL ducklake_recluster({paste(all_args, collapse = ', ')})")

  message("Reclustering ", qname, "...")
  DBI::dbExecute(con, sql)
  message("Reclustering complete.")

  invisible(qname)
}


# ==============================================================================
# Iceberg Export
# ==============================================================================

#' Export a DuckLake table as Iceberg format
#'
#' @description Exports a DuckLake table to Iceberg format for compatibility
#' with other data lakehouse engines (Spark, Trino, Presto, etc.). Creates
#' Iceberg metadata files alongside the existing parquet data files.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param path Output path for Iceberg metadata. If NULL, uses the table's
#'   existing data path with an `iceberg/` subdirectory.
#' @param catalog_type Type of Iceberg catalog to generate: "hadoop" (default),
#'   "hive", or "rest".
#' @return Invisibly returns the output path
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Export to Iceberg format
#' db_export_iceberg(table = "sales")
#'
#' # Export to specific location
#' db_export_iceberg(table = "sales", path = "/data/iceberg/sales")
#'
#' # Export for Hive Metastore compatibility
#' db_export_iceberg(table = "sales", catalog_type = "hive")
#' }
#' @seealso [db_iceberg_metadata()] to view Iceberg metadata
#' @export
db_export_iceberg <- function(schema = "main",
                              table,
                              path = NULL,
                              catalog_type = c("hadoop", "hive", "rest")) {
  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")
  catalog_type <- match.arg(catalog_type)

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", table)

  args <- c(.db_sql_quote(catalog), .db_sql_quote(table))
  named_args <- c(glue::glue("schema => {.db_sql_quote(schema)}"))

  if (!is.null(path)) {
    named_args <- c(named_args, glue::glue("path => {.db_sql_quote(path)}"))
  }

  named_args <- c(named_args, glue::glue("catalog_type => {.db_sql_quote(catalog_type)}"))

  all_args <- c(args, named_args)
  sql <- glue::glue("CALL ducklake_export_iceberg({paste(all_args, collapse = ', ')})")

  message("Exporting ", qname, " to Iceberg format...")
  result <- tryCatch({
    DBI::dbGetQuery(con, sql)
  }, error = function(e) {
    stop("Iceberg export failed: ", e$message, call. = FALSE)
  })

  output_path <- if (!is.null(path)) path else result$path[1]
  message("Iceberg export complete: ", output_path)

  invisible(output_path)
}


#' Get Iceberg metadata for a DuckLake table
#'
#' @description Returns Iceberg-compatible metadata for a DuckLake table,
#' including schema, partitioning, and snapshot information in Iceberg format.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return A list containing Iceberg metadata:
#'   - `table_uuid`: Unique table identifier
#'   - `schema`: Iceberg schema definition
#'   - `partition_spec`: Partitioning specification
#'   - `sort_order`: Sort order specification
#'   - `current_snapshot_id`: Current snapshot ID
#'   - `snapshots`: List of available snapshots
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Get Iceberg metadata
#' meta <- db_iceberg_metadata(table = "sales")
#' meta$schema
#' meta$partition_spec
#' }
#' @seealso [db_export_iceberg()] to export as Iceberg format
#' @export
db_iceberg_metadata <- function(schema = "main", table) {
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

  qname <- paste0(catalog, ".", schema, ".", table)

  sql <- glue::glue("FROM ducklake_iceberg_metadata({.db_sql_quote(catalog)}, {.db_sql_quote(table)}, schema => {.db_sql_quote(schema)})")

  result <- tryCatch({
    DBI::dbGetQuery(con, sql)
  }, error = function(e) {
    stop("Failed to get Iceberg metadata: ", e$message, call. = FALSE)
  })

  if (nrow(result) == 0) {
    stop("Table '", qname, "' not found.", call. = FALSE)
  }

  # Parse JSON fields if present
  parse_json_col <- function(col) {
    if (is.null(col) || is.na(col)) return(NULL)
    tryCatch(jsonlite::fromJSON(col), error = function(e) col)
  }

  list(
    table_uuid = result$table_uuid[1],
    schema = parse_json_col(result$schema[1]),
    partition_spec = parse_json_col(result$partition_spec[1]),
    sort_order = parse_json_col(result$sort_order[1]),
    current_snapshot_id = result$current_snapshot_id[1],
    snapshots = parse_json_col(result$snapshots[1])
  )
}
