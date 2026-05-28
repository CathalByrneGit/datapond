# R/advanced.R
# Advanced DuckLake features: inlining, clustering, and Iceberg export

# ==============================================================================
# Data Inlining
# ==============================================================================

#' Flush inlined data to parquet files
#'
#' @description Writes inlined data (small inserts/deletes stored in the catalog)
#' to parquet files. DuckLake automatically inlines writes with fewer rows than
#' `data_inlining_row_limit` (default 10). Use this function to consolidate
#' inlined data into proper parquet files.
#'
#' @param schema Schema name (default "main"). Use NULL for all schemas.
#' @param table Table name. Use NULL for all tables in the schema.
#' @return Invisibly returns a data.frame with schema_name, table_name, rows_flushed
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Small writes are automatically inlined
#' db_write(small_batch, table = "events", mode = "append")  # < 10 rows
#'
#' # Flush inlined data for a specific table
#' db_flush_inlined(table = "events")
#'
#' # Flush all inlined data in a schema
#' db_flush_inlined(schema = "raw", table = NULL)
#'
#' # Flush all inlined data in catalog
#' db_flush_inlined(schema = NULL, table = NULL)
#' }
#' @seealso [db_set_inline_threshold()] to configure inlining threshold
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
    args <- c(args, glue::glue("table_name => {.db_sql_quote(table)}"))
  }

  if (!is.null(schema)) {
    schema <- .db_validate_name(schema, "schema")
    args <- c(args, glue::glue("schema_name => {.db_sql_quote(schema)}"))
  }

  sql <- glue::glue("SELECT * FROM ducklake_flush_inlined_data({paste(args, collapse = ', ')})")

  result <- tryCatch({
    DBI::dbGetQuery(con, sql)
  }, error = function(e) {
    # Some versions return an error when no data to flush
    if (grepl("no inlined data|empty|no rows", e$message, ignore.case = TRUE)) {
      return(data.frame(schema_name = character(), table_name = character(), rows_flushed = integer()))
    }
    stop("Flush failed: ", e$message, call. = FALSE)
  })

  if (is.null(result) || nrow(result) == 0) {
    message("No inlined data to flush.")
  } else {
    total_rows <- sum(result$rows_flushed, na.rm = TRUE)
    if (total_rows > 0) {
      message("Flushed ", total_rows, " rows from ", nrow(result), " table(s) to parquet files.")
    } else {
      message("No inlined data to flush.")
    }
  }

  invisible(result)
}


#' Set the inline threshold for a table, schema, or globally
#'
#' @description Configures the row count threshold below which DuckLake automatically
#' inlines small writes to the catalog instead of writing parquet files.
#' The setting is persisted in DuckLake metadata.
#'
#' @param schema Schema name (default "main"). Use NULL for global setting.
#' @param table Table name. Use NULL for schema-level or global setting.
#' @param threshold Number of rows threshold for inlining. Writes with fewer
#'   rows than this are inlined. Use 0 to disable inlining. Default is 10.
#' @return Invisibly returns TRUE on success
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Set threshold for a specific table
#' db_set_inline_threshold(table = "events", threshold = 50)
#'
#' # Set threshold for entire schema
#' db_set_inline_threshold(schema = "raw", table = NULL, threshold = 100)
#'
#' # Set global threshold
#' db_set_inline_threshold(schema = NULL, table = NULL, threshold = 20)
#'
#' # Disable inlining for a table (all writes go directly to parquet)
#' db_set_inline_threshold(table = "events", threshold = 0)
#' }
#' @seealso [db_flush_inlined()] to manually flush inlined data
#' @export
db_set_inline_threshold <- function(schema = "main", table = NULL, threshold = 10) {
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

  # Build set_option call: CALL catalog.set_option('data_inlining_row_limit', N, ...)
  args <- c(
    "'data_inlining_row_limit'",
    as.integer(threshold)
  )

  scope_desc <- "globally"

  if (!is.null(table)) {
    table <- .db_validate_name(table, "table")
    args <- c(args, glue::glue("table_name := {.db_sql_quote(table)}"))
    scope_desc <- paste0("for table ", table)
  }

  if (!is.null(schema)) {
    schema <- .db_validate_name(schema, "schema")
    args <- c(args, glue::glue("schema := {.db_sql_quote(schema)}"))
    if (is.null(table)) {
      scope_desc <- paste0("for schema ", schema)
    } else {
      scope_desc <- paste0("for ", schema, ".", table)
    }
  }

  sql <- glue::glue("CALL {catalog}.set_option({paste(args, collapse = ', ')})")
  DBI::dbExecute(con, sql)

  message("Set inline threshold to ", threshold, " rows ", scope_desc, ".")
  invisible(TRUE)
}


# ==============================================================================
# Clustering / Sorted Tables
# ==============================================================================

#' Set sort order for a table
#'
#' @description Configures the sort order for data files in a table. When new
#' data is written, it will be sorted by these columns within each file.
#' This improves query performance for range scans and filters on the
#' sorted columns.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param columns Character vector of column names to sort by, in order of priority.
#'   Use NULL to remove sort order.
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Set sort order for time-series queries
#' db_set_clustering(table = "events", columns = c("event_date", "user_id"))
#'
#' # Remove sort order
#' db_set_clustering(table = "events", columns = NULL)
#' }
#' @seealso [db_recluster()] to apply sort order to existing data, [db_write()]
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
    sql <- glue::glue("ALTER TABLE {qname} RESET SORTED BY")
    DBI::dbExecute(con, sql)
    message("Removed sort order from ", qname)
  } else {
    if (!is.character(columns)) {
      stop("columns must be a character vector.", call. = FALSE)
    }
    sort_clause <- paste(columns, collapse = ", ")
    sql <- glue::glue("ALTER TABLE {qname} SET SORTED BY ({sort_clause})")
    DBI::dbExecute(con, sql)
    message("Set sorted order for ", qname, ": ", sort_clause)
  }

  invisible(qname)
}


#' Re-cluster table data
#'
#' @description Rewrites table data files to match the current clustering order.
#' Use this after setting clustering on a table that already contains data,
#' or after many appends have fragmented the sort order.
#'
#' DuckLake automatically sorts data during compaction based on the table's
#' sort order (SET SORTED BY), so this function compacts files to re-sort.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param max_files Maximum number of compaction operations per call.
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
#' @seealso [db_set_clustering()] to configure clustering order, [db_compact()]
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

  # DuckLake sorts data during compaction based on SET SORTED BY order
  # Use ducklake_merge_adjacent_files to compact and re-sort
  args <- c(.db_sql_quote(catalog), .db_sql_quote(table))

  named_args <- c(glue::glue("schema => {.db_sql_quote(schema)}"))
  if (!is.null(max_files)) {
    if (!is.numeric(max_files) || max_files < 1) {
      stop("max_files must be a positive integer.", call. = FALSE)
    }
    named_args <- c(named_args, glue::glue("max_compacted_files => {as.integer(max_files)}"))
  }

  all_args <- c(args, named_args)
  sql <- glue::glue("CALL ducklake_merge_adjacent_files({paste(all_args, collapse = ', ')})")

  message("Reclustering ", qname, " (compacting with sort order)...")
  DBI::dbExecute(con, sql)
  message("Reclustering complete.")

  invisible(qname)
}


# ==============================================================================
# Iceberg Export
# ==============================================================================

#' Export a DuckLake table as Iceberg format (EXPERIMENTAL)
#'
#' @description Exports a DuckLake table to Iceberg format for compatibility
#' with other data lakehouse engines (Spark, Trino, Presto, etc.).
#'
#' **Note:** This is experimental. DuckLake 0.3+ supports Iceberg interoperability
#' via `COPY FROM DATABASE ducklake TO iceberg_catalog`. This function attempts
#' to use internal DuckLake Iceberg functions which may not be available.
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
    if (grepl("does not exist", e$message, ignore.case = TRUE)) {
      stop("Iceberg export not available in this DuckLake version. ",
           "Check DuckLake documentation for Iceberg compatibility.", call. = FALSE)
    }
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
#' **Note:** This feature requires DuckLake functions that may not yet be
#' available in all versions. Check DuckLake documentation for compatibility.
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
    if (grepl("does not exist", e$message, ignore.case = TRUE)) {
      stop("Iceberg metadata not available in this DuckLake version. ",
           "Check DuckLake documentation for Iceberg compatibility.", call. = FALSE)
    }
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
