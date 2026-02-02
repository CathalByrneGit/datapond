# R/maintenance.R

# ---- Vacuum / Cleanup ----

#' Vacuum old snapshots from DuckLake
#' 
#' @description Removes old snapshots and their associated data files that are
#' no longer needed. This reclaims storage space by deleting data that is not
#' referenced by any snapshot within the retention period.
#' 
#' @param older_than Snapshots older than this will be removed. Can be:
#'   - A difftime or lubridate duration (e.g. `as.difftime(7, units = "days")`)
#'   - A character string parseable by DuckDB (e.g. "7 days", "1 month")
#'   - A POSIXct timestamp (snapshots before this time are removed)
#' @param dry_run If TRUE (default), reports what would be deleted without actually deleting.
#'   Set to FALSE to perform the actual cleanup.
#' @return A data.frame summarising what was (or would be) cleaned up
#' @examples
#' \dontrun{
#' db_lake_connect()
#' 
#' # See what would be cleaned up (dry run)
#' db_vacuum(older_than = "30 days")
#' 
#' # Actually clean up
#' db_vacuum(older_than = "30 days", dry_run = FALSE)
#' }
#' @export
db_vacuum <- function(older_than = "30 days", dry_run = TRUE) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }
  
  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Vacuum is only available for DuckLake.", call. = FALSE)
  }
  
  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
  }
  
  # Convert older_than to a timestamp string for DuckDB
  if (inherits(older_than, "POSIXct")) {
    cutoff_ts <- format(older_than, "%Y-%m-%d %H:%M:%S")
  } else if (inherits(older_than, "difftime")) {
    cutoff_ts <- format(Sys.time() - older_than, "%Y-%m-%d %H:%M:%S")
  } else if (is.character(older_than)) {
    # Let DuckDB parse interval strings like "7 days", "1 month"
    cutoff_ts <- NULL
  } else {
    stop("older_than must be a POSIXct, difftime, or character interval string.", call. = FALSE)
  }
  
  # Get current snapshot info before vacuum
  snapshots_before <- DBI::dbGetQuery(con, 
    glue::glue("FROM ducklake_snapshots({.db_sql_quote(catalog)})")
  )
  
  if (nrow(snapshots_before) == 0) {
    message("No snapshots found in catalog.")
    return(invisible(data.frame()))
  }
  
  # Build the vacuum SQL
  if (is.null(cutoff_ts)) {
    # Use interval string directly
    vacuum_sql <- glue::glue(
      "CALL ducklake_vacuum({.db_sql_quote(catalog)}, INTERVAL {.db_sql_quote(older_than)})"
    )
  } else {
    vacuum_sql <- glue::glue(
      "CALL ducklake_vacuum({.db_sql_quote(catalog)}, TIMESTAMP {.db_sql_quote(cutoff_ts)})"
    )
  }
  
  if (dry_run) {
    # Calculate what would be removed
    if (is.null(cutoff_ts)) {
      # Parse the interval to calculate cutoff
      # Cast NOW() to TIMESTAMP to enable interval arithmetic
      cutoff_result <- DBI::dbGetQuery(con, 
        glue::glue("SELECT NOW()::TIMESTAMP - INTERVAL {.db_sql_quote(older_than)} AS cutoff")
      )
      cutoff_time <- cutoff_result$cutoff[1]
    } else {
      cutoff_time <- as.POSIXct(cutoff_ts)
    }
    
    # Find snapshots that would be removed
    to_remove <- snapshots_before[snapshots_before$snapshot_time < cutoff_time, ]
    to_keep <- snapshots_before[snapshots_before$snapshot_time >= cutoff_time, ]
    
    cat("\n-- DRY RUN: Vacuum Preview --\n\n")
    cat("Catalog:", catalog, "\n")
    cat("Cutoff:", as.character(cutoff_time), "\n\n")
    cat("Snapshots to REMOVE:", nrow(to_remove), "\n")
    cat("Snapshots to KEEP:  ", nrow(to_keep), "\n\n")
    
    if (nrow(to_remove) > 0) {
      cat("Snapshots that would be removed:\n")
      print(to_remove[, c("snapshot_id", "snapshot_time", "commit_message")], row.names = FALSE)
    }
    
    cat("\nRun with dry_run = FALSE to perform the actual cleanup.\n")
    
    return(invisible(to_remove))
  }
  
  # Actually run vacuum
  DBI::dbExecute(con, vacuum_sql)
  
  # Get snapshot info after vacuum
 snapshots_after <- DBI::dbGetQuery(con, 
    glue::glue("FROM ducklake_snapshots({.db_sql_quote(catalog)})")
  )
  
  removed_count <- nrow(snapshots_before) - nrow(snapshots_after)
  
  message("Vacuum complete. Removed ", removed_count, " snapshot(s).")
  
  invisible(data.frame(
    snapshots_before = nrow(snapshots_before),
    snapshots_after = nrow(snapshots_after),
    snapshots_removed = removed_count
  ))
}


# ---- Rollback / Time Travel ----
 
#' Rollback a table to a previous snapshot
#' 
#' @description Restores a table to its state at a specific snapshot version or timestamp.
#' This creates a new snapshot with the rolled-back data.
#' 
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param version Snapshot version to rollback to (integer)
#' @param timestamp Timestamp to rollback to (POSIXct or character string)
#' @param commit_author Optional author for the rollback commit
#' @param commit_message Optional message for the rollback commit (defaults to auto-generated)
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' db_lake_connect()
#' 
#' # Rollback to a specific version
#' db_rollback(table = "products", version = 5)
#' 
#' # Rollback to a specific time
#' db_rollback(table = "products", timestamp = "2025-01-15 00:00:00")
#' }
#' @export
db_rollback <- function(schema = "main",
                        table,
                        version = NULL,
                        timestamp = NULL,
                        commit_author = NULL,
                        commit_message = NULL) {
  
  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")
  
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }
  
  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Rollback is only available for DuckLake.", call. = FALSE)
  }
  
  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
  }
  
  if (is.null(version) && is.null(timestamp)) {
    stop("Must specify either 'version' or 'timestamp' to rollback to.", call. = FALSE)
  }
  
  if (!is.null(version) && !is.null(timestamp)) {
    stop("Specify only one of 'version' or 'timestamp', not both.", call. = FALSE)
  }
  
  qname <- glue::glue("{catalog}.{schema}.{table}")
  
  # Build the AT clause for time travel
  if (!is.null(version)) {
    at_clause <- glue::glue("AT (VERSION => {as.integer(version)})")
    rollback_desc <- paste0("version ", version)
  } else {
    if (inherits(timestamp, "POSIXct")) {
      ts_str <- format(timestamp, "%Y-%m-%d %H:%M:%S")
    } else {
      ts_str <- as.character(timestamp)
    }
    at_clause <- glue::glue("AT (TIMESTAMP => {.db_sql_quote(ts_str)})")
    rollback_desc <- paste0("timestamp ", ts_str)
  }
  
  # Default commit message if not provided
  if (is.null(commit_message)) {
    commit_message <- paste0("Rollback to ", rollback_desc)
  }
  
  # Start transaction
  DBI::dbExecute(con, "BEGIN")
  
  # Set commit metadata
  author_val <- if (is.null(commit_author)) "NULL" else .db_sql_quote(commit_author)
  msg_val <- .db_sql_quote(commit_message)
  DBI::dbExecute(con, glue::glue("CALL ducklake_set_commit_message({.db_sql_quote(catalog)}, {author_val}, {msg_val})"))
  
  # Perform rollback using CREATE OR REPLACE with time travel
  rollback_sql <- glue::glue("CREATE OR REPLACE TABLE {qname} AS SELECT * FROM {qname} {at_clause}")
  
  tryCatch({
    DBI::dbExecute(con, rollback_sql)
    DBI::dbExecute(con, "COMMIT")
  }, error = function(e) {
    try(DBI::dbExecute(con, "ROLLBACK"), silent = TRUE)
    stop(e$message, call. = FALSE)
  })
  
  message("Rolled back ", qname, " to ", rollback_desc)
  
  invisible(as.character(qname))
}


# ---- Diff / Compare Snapshots ----

#' Compare a table between two snapshots
#' 
#' @description Shows the differences in a table between two snapshot versions
#' db_diff() is set-based (EXCEPT), so duplicates don’t count as “added”.
#' Your append produced 10 duplicate rows + 2 genuinely new distinct rows, so it reports 2 added.
#' or timestamps. Returns added, removed, and (optionally) changed rows.
#' 
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param from_version Starting snapshot version (integer) or NULL to use from_timestamp
#' @param to_version Ending snapshot version (integer, default: current) or NULL to use to_timestamp
#' @param from_timestamp Starting timestamp (alternative to from_version)
#' @param to_timestamp Ending timestamp (alternative to to_version, default: current)
#' @param key_cols Character vector of columns that uniquely identify rows.
#'   If provided, enables detection of modified rows (not just added/removed).
#' @param collect If TRUE (default), returns collected data.frames. 
#'   If FALSE, returns lazy tbl references.
#' @return A list with components: `added`, `removed`, and (if key_cols provided) `modified`
#' @examples
#' \dontrun{
#' db_lake_connect()
#' 
#' # Compare versions 3 and 5
#' diff <- db_diff(table = "products", from_version = 3, to_version = 5)
#' diff$added
#' diff$removed
#' 
#' # Compare with key columns to see modifications
#' diff <- db_diff(table = "products", from_version = 3, to_version = 5,
#'                 key_cols = "product_id")
#' diff$modified
#' }
#' @export
db_diff <- function(schema = "main",
                    table,
                    from_version = NULL,
                    to_version = NULL,
                    from_timestamp = NULL,
                    to_timestamp = NULL,
                    key_cols = NULL,
                    collect = TRUE) {
  
  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")
  
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }
  
  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Diff is only available for DuckLake.", call. = FALSE)
  }
  
  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
  }
  
  # Need at least a 'from' reference
  if (is.null(from_version) && is.null(from_timestamp)) {
    stop("Must specify either 'from_version' or 'from_timestamp'.", call. = FALSE)
  }
  
  qname <- glue::glue("{catalog}.{schema}.{table}")
  
  # Build FROM clause for the old snapshot
  if (!is.null(from_version)) {
    from_at <- glue::glue("AT (VERSION => {as.integer(from_version)})")
  } else {
    if (inherits(from_timestamp, "POSIXct")) {
      ts_str <- format(from_timestamp, "%Y-%m-%d %H:%M:%S")
    } else {
      ts_str <- as.character(from_timestamp)
    }
    from_at <- glue::glue("AT (TIMESTAMP => {.db_sql_quote(ts_str)})")
  }
  
  # Build FROM clause for the new snapshot (current if not specified)
  if (!is.null(to_version)) {
    to_at <- glue::glue("AT (VERSION => {as.integer(to_version)})")
    to_ref <- glue::glue("{qname} {to_at}")
  } else if (!is.null(to_timestamp)) {
    if (inherits(to_timestamp, "POSIXct")) {
      ts_str <- format(to_timestamp, "%Y-%m-%d %H:%M:%S")
    } else {
      ts_str <- as.character(to_timestamp)
    }
    to_at <- glue::glue("AT (TIMESTAMP => {.db_sql_quote(ts_str)})")
    to_ref <- glue::glue("{qname} {to_at}")
  } else {
    # Current version
    to_at <- ""
    to_ref <- qname
  }
  
  from_ref <- glue::glue("{qname} {from_at}")
  
  # ADDED: rows in 'to' but not in 'from'
  added_sql <- glue::glue("SELECT * FROM {to_ref} EXCEPT SELECT * FROM {from_ref}")
  
  # REMOVED: rows in 'from' but not in 'to'
  removed_sql <- glue::glue("SELECT * FROM {from_ref} EXCEPT SELECT * FROM {to_ref}")
  
  result <- list()
  
  if (collect) {
    result$added <- DBI::dbGetQuery(con, added_sql)
    result$removed <- DBI::dbGetQuery(con, removed_sql)
  } else {
    result$added <- dplyr::tbl(con, dplyr::sql(added_sql))
    result$removed <- dplyr::tbl(con, dplyr::sql(removed_sql))
  }
  
  # If key_cols provided, find modified rows (same key, different values)
  if (!is.null(key_cols)) {
    if (!is.character(key_cols) || length(key_cols) < 1) {
      stop("key_cols must be a character vector of column names.", call. = FALSE)
    }
    
    key_sql <- paste(key_cols, collapse = ", ")
    join_cond <- paste(glue::glue("old_tbl.{key_cols} = new_tbl.{key_cols}"), collapse = " AND ")
    
    # Modified: same key exists in both, but row is different
    # (key in both added and removed sets)
    modified_sql <- glue::glue("
      SELECT new_tbl.* 
      FROM ({added_sql}) AS new_tbl
      INNER JOIN ({removed_sql}) AS old_tbl
      ON {join_cond}
    ")
    
    if (collect) {
      result$modified <- DBI::dbGetQuery(con, modified_sql)
    } else {
      result$modified <- dplyr::tbl(con, dplyr::sql(modified_sql))
    }
  }
  
  # Summary
  if (collect) {
    cat("\n-- Diff Summary --\n")
    cat("Added rows:   ", nrow(result$added), "\n")
    cat("Removed rows: ", nrow(result$removed), "\n")
    if (!is.null(key_cols)) {
      cat("Modified rows:", nrow(result$modified), "\n")
    }
    cat("\n")
  }
  
  result
}


# ---- Query Helper ----

#' Run arbitrary SQL and return results
#' 
#' @description Escape hatch for power users who need to run custom SQL queries.
#' @param sql SQL query string
#' @param collect If TRUE (default), returns a collected data.frame.
#'   If FALSE, returns a lazy tbl reference.
#' @return Query results as data.frame or lazy tbl
#' @examples
#' \dontrun{
#' db_lake_connect()
#' 
#' # Run a custom query
#' db_query("SELECT * FROM main.products WHERE price > 100")
#' 
#' # Get a lazy reference
#' lazy_result <- db_query("SELECT * FROM main.products", collect = FALSE)
#' }
#' @export
db_query <- function(sql, collect = TRUE) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }
  
  if (!is.character(sql) || length(sql) != 1 || !nzchar(sql)) {
    stop("sql must be a non-empty string.", call. = FALSE)
  }
  
  if (collect) {
    DBI::dbGetQuery(con, sql)
  } else {
    dplyr::tbl(con, dplyr::sql(sql))
  }
}
