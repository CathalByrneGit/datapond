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
#' db_connect()
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
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
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


# ---- File Compaction ----

#' Compact small files in a DuckLake table
#'
#' @description Merges small Parquet files into larger ones to improve query
#' performance. When data is written in small batches, DuckLake creates many
#' small files which slows down reads. Compaction consolidates these files.
#'
#' **When to compact:**
#' - After many small inserts (e.g., streaming data, row-by-row imports)
#' - When `db_file_stats()` shows high file counts with small average sizes
#' - Before running large analytical queries on frequently-updated tables
#'
#' **Important notes:**
#' - Compaction is memory-intensive; use `max_files` to limit batch size
#' - Files with different schema versions cannot be merged together
#' - Old files are not immediately deleted; run `db_cleanup_files()` after
#'
#' @param schema Schema name (default "main"). Use NULL to compact all schemas.
#' @param table Table name. Use NULL to compact all tables in the schema.
#' @param max_files Maximum number of files to compact in one operation.
#'   Lower values use less memory. Default NULL compacts all eligible files.
#' @return Invisibly returns a list with compaction results
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Check if compaction is needed
#' db_file_stats()
#'
#' # Compact a specific table
#' db_compact(table = "imports")
#'
#' # Compact with memory limit (process 500 files at a time)
#' db_compact(table = "imports", max_files = 500)
#'
#' # Compact all tables in a schema
#' db_compact(schema = "trade")
#'
#' # Compact entire catalog
#' db_compact()
#'
#' # Clean up old files after compaction
#' db_cleanup_files()
#' }
#' @seealso [db_file_stats()] to check file statistics before compacting,
#'   [db_cleanup_files()] to remove old files after compaction
#' @export
db_compact <- function(schema = "main", table = NULL, max_files = NULL) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  # Build the SQL call
  args <- c(.db_sql_quote(catalog))

  if (!is.null(table)) {
    table <- .db_validate_name(table, "table")
    args <- c(args, .db_sql_quote(table))
  }

  named_args <- c()
  if (!is.null(schema)) {
    schema <- .db_validate_name(schema, "schema")
    named_args <- c(named_args, glue::glue("schema => {.db_sql_quote(schema)}"))
  }
  if (!is.null(max_files)) {
    if (!is.numeric(max_files) || max_files < 1) {
      stop("max_files must be a positive integer.", call. = FALSE)
    }
    named_args <- c(named_args, glue::glue("max_compacted_files => {as.integer(max_files)}"))
  }

  all_args <- c(args, named_args)
  sql <- glue::glue("CALL ducklake_merge_adjacent_files({paste(all_args, collapse = ', ')})")

  # Get file stats before compaction for reporting
  stats_before <- tryCatch(
    db_file_stats(schema = schema, table = table),
    error = function(e) NULL
  )

  # Run compaction
  message("Compacting files...")
  if (!is.null(table)) {
    message("  Table: ", if (!is.null(schema)) paste0(schema, ".") else "", table)
  } else if (!is.null(schema)) {
    message("  Schema: ", schema)
  } else {
    message("  Entire catalog: ", catalog)
  }
  if (!is.null(max_files)) {
    message("  Max files per batch: ", max_files)
  }

  tryCatch({
    DBI::dbExecute(con, sql)
  }, error = function(e) {
    # Check for common issues
    if (grepl("schema version", e$message, ignore.case = TRUE)) {
      stop(
        "Compaction failed: Files have incompatible schema versions.\n",
        "This happens when table structure changed between inserts.\n",
        "Consider running compaction more frequently after bulk inserts.\n\n",
        "Original error: ", e$message,
        call. = FALSE
      )
    }
    stop("Compaction failed: ", e$message, call. = FALSE)
  })

  # Get file stats after compaction for reporting
  stats_after <- tryCatch(
    db_file_stats(schema = schema, table = table),
    error = function(e) NULL
  )

  # Report results
  if (!is.null(stats_before) && !is.null(stats_after)) {
    files_before <- sum(stats_before$file_count, na.rm = TRUE)
    files_after <- sum(stats_after$file_count, na.rm = TRUE)
    files_reduced <- files_before - files_after

    if (files_reduced > 0) {
      message("Compaction complete:")
      message("  Files before: ", files_before)
      message("  Files after:  ", files_after)
      message("  Files merged: ", files_reduced)
      message("\nRun db_cleanup_files() to remove old files from storage.")
    } else {
      message("Compaction complete. No files were eligible for merging.")
      message("(Files may have incompatible schema versions)")
    }
  } else {
    message("Compaction complete.")
    message("Run db_cleanup_files() to remove old files from storage.")
  }

  invisible(list(
    stats_before = stats_before,
    stats_after = stats_after
  ))
}


#' Get file statistics for DuckLake tables
#'
#' @description Returns information about the Parquet files backing each table,
#' including file counts, sizes, and row counts. Use this to identify tables
#' that would benefit from compaction.
#'
#' **Indicators that compaction may help:**
#' - High file count with small average file size (< 10 MB)
#' - Many more files than expected for the data volume
#' - Slow query performance on tables with many files
#'
#' @param schema Schema name (default "main"). Use NULL for all schemas.
#' @param table Table name. Use NULL for all tables.
#' @return A data.frame with columns:
#'   - `schema_name`: Schema containing the table
#'   - `table_name`: Table name
#'   - `file_count`: Number of Parquet files
#'   - `total_rows`: Total row count across all files
#'   - `total_bytes`: Total size in bytes
#'   - `avg_file_bytes`: Average file size
#'   - `avg_rows_per_file`: Average rows per file
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Check all tables
#' db_file_stats()
#'
#' # Check a specific table
#' db_file_stats(table = "imports")
#'
#' # Find tables needing compaction (many small files)
#' stats <- db_file_stats()
#' stats[stats$file_count > 100 & stats$avg_file_bytes < 1e7, ]
#' }
#' @seealso [db_compact()] to merge small files
#' @export
db_file_stats <- function(schema = "main", table = NULL) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  # Query DuckLake table info which includes file statistics
  sql <- glue::glue("FROM ducklake_table_info({.db_sql_quote(catalog)})")
  info <- DBI::dbGetQuery(con, sql)

  if (nrow(info) == 0) {
    message("No tables found in catalog.")
    return(invisible(data.frame(
      schema_name = character(0),
      table_name = character(0),
      file_count = integer(0),
      total_rows = numeric(0),
      total_bytes = numeric(0),
      avg_file_bytes = numeric(0),
      avg_rows_per_file = numeric(0)
    )))
  }

  # Filter by schema if specified
 if (!is.null(schema)) {
    schema <- .db_validate_name(schema, "schema")
    info <- info[info$schema_name == schema, , drop = FALSE]
  }

  # Filter by table if specified
  if (!is.null(table)) {
    table <- .db_validate_name(table, "table")
    info <- info[info$table_name == table, , drop = FALSE]
  }

  if (nrow(info) == 0) {
    message("No matching tables found.")
    return(invisible(data.frame(
      schema_name = character(0),
      table_name = character(0),
      file_count = integer(0),
      total_rows = numeric(0),
      total_bytes = numeric(0),
      avg_file_bytes = numeric(0),
      avg_rows_per_file = numeric(0)
    )))
  }

  # Build result with useful statistics
  # ducklake_table_info returns: schema_name, table_name, estimated_size, file_count, etc.
  result <- data.frame(
    schema_name = info$schema_name,
    table_name = info$table_name,
    file_count = if ("file_count" %in% names(info)) info$file_count else NA_integer_,
    total_rows = if ("row_count" %in% names(info)) info$row_count else NA_real_,
    total_bytes = if ("estimated_size" %in% names(info)) info$estimated_size else NA_real_,
    stringsAsFactors = FALSE
  )

  # Calculate derived statistics
  result$avg_file_bytes <- ifelse(
    result$file_count > 0,
    result$total_bytes / result$file_count,
    NA_real_
  )
  result$avg_rows_per_file <- ifelse(
    result$file_count > 0,
    result$total_rows / result$file_count,
    NA_real_
  )

  result
}


#' Clean up orphaned files from DuckLake storage
#'
#' @description Removes data files that are no longer referenced by any snapshot.
#' Run this after `db_vacuum()` or `db_compact()` to reclaim disk space.
#'
#' **When files become orphaned:**
#' - After `db_vacuum()` removes old snapshots
#' - After `db_compact()` merges files (old small files become orphaned)
#' - After failed transactions that wrote partial data
#'
#' @param dry_run If TRUE (default), shows what would be deleted without deleting.
#'   Set to FALSE to actually remove files.
#' @return Invisibly returns the count of files cleaned up (or that would be)
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Compact files then clean up
#' db_compact(table = "imports")
#' db_cleanup_files(dry_run = FALSE)
#'
#' # Vacuum old snapshots then clean up
#' db_vacuum(older_than = "30 days", dry_run = FALSE)
#' db_cleanup_files(dry_run = FALSE)
#' }
#' @seealso [db_vacuum()] to remove old snapshots, [db_compact()] to merge files
#' @export
db_cleanup_files <- function(dry_run = TRUE) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  if (dry_run) {
    # DuckLake doesn't have a built-in dry-run for cleanup, so we inform the user
    cat("\n-- DRY RUN: Cleanup Preview --\n\n")
    cat("Catalog:", catalog, "\n\n")
    cat("This operation will remove orphaned files from storage.\n")
    cat("Orphaned files are created by:\
")
    cat("  - db_vacuum() removing old snapshots\n")
    cat("  - db_compact() merging small files\n")
    cat("  - Failed or rolled-back transactions\n\n")
    cat("Run with dry_run = FALSE to perform the actual cleanup.\n\n")

    return(invisible(NA_integer_))
  }

  sql <- glue::glue("CALL ducklake_cleanup_old_files({.db_sql_quote(catalog)})")

  tryCatch({
    DBI::dbExecute(con, sql)
    message("Cleanup complete. Orphaned files have been removed.")
  }, error = function(e) {
    stop("Cleanup failed: ", e$message, call. = FALSE)
  })

  invisible(TRUE)
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
#' db_connect()
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
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
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
#' db_diff() is set-based (EXCEPT), so duplicates don't count as "added".
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
#' db_connect()
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
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
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
#' db_connect()
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
    stop("Not connected. Use db_connect() first.", call. = FALSE)
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
