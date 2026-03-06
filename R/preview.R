# R/preview.R
# Preview write operations before executing

#' Preview a DuckLake write operation
#'
#' @description Shows what would happen if you ran `db_write()` without
#' actually writing any data.
#'
#' @inheritParams db_write
#' @return A list with preview information (invisibly), also prints a summary
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' db_preview_write(my_data, table = "products", mode = "overwrite")
#'
#' # Preview with partitioning
#' db_preview_write(my_data, table = "sales",
#'                  partition_by = c("year", "month"))
#' }
#' @export
#' @importFrom stats setNames
db_preview_write <- function(data,
                             schema = "main",
                             table,
                             mode = c("overwrite", "append"),
                             partition_by = NULL) {

  mode <- match.arg(mode)

  if (!is.data.frame(data)) {
    stop("data must be a data.frame / tibble.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")

  # Validate partition_by
  if (!is.null(partition_by)) {
    if (!is.character(partition_by) || length(partition_by) == 0) {
      stop("partition_by must be a non-empty character vector.", call. = FALSE)
    }
    missing_cols <- setdiff(partition_by, names(data))
    if (length(missing_cols) > 0) {
      stop("partition_by columns not found in data: ",
           paste(missing_cols, collapse = ", "),
           call. = FALSE)
    }
    if (mode == "append") {
      stop("partition_by cannot be used with mode = 'append'.", call. = FALSE)
    }
  }

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  qname <- paste0(catalog, ".", schema, ".", table)

  # Check if table exists
  table_exists <- .db_table_exists(con, catalog, schema, table)

  # Get existing partitioning if table exists
  existing_partitioning <- NULL
  if (table_exists) {
    existing_partitioning <- .db_get_partitioning_internal(con, catalog, schema, table)
  }

  # Determine effective partitioning
  effective_partition_by <- partition_by
  if (mode == "overwrite" && table_exists && is.null(partition_by) && !is.null(existing_partitioning)) {
    effective_partition_by <- existing_partitioning
  }

  preview <- list(
    mode = mode,
    catalog = catalog,
    schema = schema,
    table = table,
    qname = qname,
    table_exists = table_exists,
    incoming = list(
      rows = nrow(data),
      cols = ncol(data),
      columns = names(data),
      types = vapply(data, function(x) class(x)[1], character(1))
    ),
    existing = NULL,
    schema_changes = NULL,
    partition_by = partition_by,
    existing_partitioning = existing_partitioning,
    effective_partition_by = effective_partition_by
  )

  # Get existing table info
  if (table_exists) {
    existing_info <- tryCatch({
      # Get columns
      cols_sql <- glue::glue("
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = {.db_sql_quote(catalog)}
          AND table_schema = {.db_sql_quote(schema)}
          AND table_name = {.db_sql_quote(table)}
        ORDER BY ordinal_position
      ")
      cols_df <- DBI::dbGetQuery(con, cols_sql)

      # Get row count
      count_sql <- glue::glue("SELECT COUNT(*) as n FROM {qname}")
      count_df <- DBI::dbGetQuery(con, count_sql)

      list(
        rows = count_df$n[1],
        cols = nrow(cols_df),
        columns = cols_df$column_name,
        types = setNames(cols_df$data_type, cols_df$column_name)
      )
    }, error = function(e) NULL)

    preview$existing <- existing_info

    # Schema comparison
    if (!is.null(existing_info)) {
      new_cols <- setdiff(preview$incoming$columns, existing_info$columns)
      removed_cols <- setdiff(existing_info$columns, preview$incoming$columns)

      preview$schema_changes <- list(
        new_columns = new_cols,
        removed_columns = removed_cols
      )
    }
  }

  # Print summary
  .print_write_preview(preview)

  invisible(preview)
}


#' Print DuckLake write preview
#' @noRd
.print_write_preview <- function(preview) {
  cat("\n")
  cat("===================================================================\n")
  cat("  WRITE PREVIEW - ", toupper(preview$mode), " MODE\n", sep = "")
  cat("===================================================================\n\n")

  cat("Target: ", preview$qname, "\n", sep = "")
  cat("Exists: ", if (preview$table_exists) "Yes" else "No (will be created)", "\n\n", sep = "")

  # Incoming data
  cat("---- Incoming Data ----------------------------------------------------------------------------------------------------\n")
  cat("Rows:    ", format(preview$incoming$rows, big.mark = ","), "\n", sep = "")
  cat("Columns: ", preview$incoming$cols, "\n\n", sep = "")

  # Existing data
  if (!is.null(preview$existing)) {
    cat("---- Existing Data ----------------------------------------------------------------------------------------------------\n")
    cat("Rows:    ", format(preview$existing$rows, big.mark = ","), "\n", sep = "")
    cat("Columns: ", preview$existing$cols, "\n\n", sep = "")
  }

  # Schema changes
  if (!is.null(preview$schema_changes)) {
    changes <- preview$schema_changes
    has_changes <- length(changes$new_columns) > 0 || length(changes$removed_columns) > 0

    if (has_changes) {
      cat("---- Schema Changes --------------------------------------------------------------------------------------------------\n")
      if (length(changes$new_columns) > 0) {
        cat("  + New columns:     ", paste(changes$new_columns, collapse = ", "), "\n", sep = "")
      }
      if (length(changes$removed_columns) > 0) {
        if (preview$mode == "overwrite") {
          cat("  - Columns to drop: ", paste(changes$removed_columns, collapse = ", "), "\n", sep = "")
        } else {
          cat("  ! Missing columns: ", paste(changes$removed_columns, collapse = ", "), " (append may fail)\n", sep = "")
        }
      }
      cat("\n")
    }
  }

  # Partitioning info
  has_partitioning <- !is.null(preview$effective_partition_by) ||
                      !is.null(preview$existing_partitioning) ||
                      !is.null(preview$partition_by)

  if (has_partitioning) {
    cat("---- Partitioning --------------------------------------------------------------------------------------------------------\n")

    if (!is.null(preview$existing_partitioning)) {
      cat("Current:   ", paste(preview$existing_partitioning, collapse = ", "), "\n", sep = "")
    } else if (preview$table_exists) {
      cat("Current:   (none)\n")
    }

    if (!is.null(preview$partition_by)) {
      cat("Requested: ", paste(preview$partition_by, collapse = ", "), "\n", sep = "")
    }

    if (!is.null(preview$effective_partition_by)) {
      if (is.null(preview$partition_by) && !is.null(preview$existing_partitioning)) {
        cat("Effective: ", paste(preview$effective_partition_by, collapse = ", "),
            " (preserved from existing)\n", sep = "")
      } else {
        cat("Effective: ", paste(preview$effective_partition_by, collapse = ", "), "\n", sep = "")
      }
    } else {
      cat("Effective: (none)\n")
    }

    cat("\n")
  }

  # Action summary
  cat("---- Action ------------------------------------------------------------------------------------------------------------------\n")
  action <- switch(preview$mode,
    overwrite = if (preview$table_exists) {
      paste0("Will REPLACE table (", format(preview$existing$rows, big.mark = ","),
             " -> ", format(preview$incoming$rows, big.mark = ","), " rows)")
    } else {
      paste0("Will CREATE new table with ", format(preview$incoming$rows, big.mark = ","), " rows")
    },
    append = if (preview$table_exists) {
      paste0("Will ADD ", format(preview$incoming$rows, big.mark = ","), " rows (total: ",
             format(preview$existing$rows + preview$incoming$rows, big.mark = ","), ")")
    } else {
      "! Table does not exist - append will FAIL"
    }
  )
  cat(action, "\n")
  cat("A new snapshot will be created.\n\n")

  cat("===================================================================\n")
  cat("Run db_write() with same parameters to execute.\n")
  cat("===================================================================\n\n")
}


#' Preview an upsert operation
#'
#' @description Shows what would happen if you ran `db_upsert()` - how many
#' rows would be inserted vs updated.
#'
#' @inheritParams db_upsert
#' @return A list with preview information (invisibly), also prints a summary
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' db_preview_upsert(my_data, table = "products", by = "product_id")
#' }
#' @export
#' @importFrom utils head
db_preview_upsert <- function(data,
                              schema = "main",
                              table,
                              by,
                              update_cols = NULL) {

  if (!is.data.frame(data)) {
    stop("data must be a data.frame / tibble.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  qname <- paste0(catalog, ".", schema, ".", table)

  # Validate by columns
  if (!is.character(by) || length(by) < 1) {
    stop("by must be a character vector of key column names.", call. = FALSE)
  }
  missing_keys <- setdiff(by, names(data))
  if (length(missing_keys) > 0) {
    stop("Key columns not found in data: ", paste(missing_keys, collapse = ", "), call. = FALSE)
  }

  # Check table exists
  table_exists <- .db_table_exists(con, catalog, schema, table)

  preview <- list(
    catalog = catalog,
    schema = schema,
    table = table,
    qname = qname,
    table_exists = table_exists,
    by = by,
    update_cols = update_cols,
    incoming = list(
      rows = nrow(data),
      columns = names(data)
    ),
    existing = NULL,
    impact = NULL
  )

  if (!table_exists) {
    preview$impact <- list(
      inserts = nrow(data),
      updates = 0,
      message = "Table does not exist - all rows will be inserted (table will be created)"
    )
  } else {
    # Get existing row count
    count_sql <- glue::glue("SELECT COUNT(*) as n FROM {qname}")
    preview$existing <- list(
      rows = DBI::dbGetQuery(con, count_sql)$n[1]
    )

    # Register incoming data temporarily
    tmp <- .db_temp_name()
    duckdb::duckdb_register(con, tmp, data)
    on.exit(try(duckdb::duckdb_unregister(con, tmp), silent = TRUE))

    # Count matches
    by_sql <- paste(by, collapse = ", ")
    join_cond <- paste(glue::glue("t.{by} = s.{by}"), collapse = " AND ")

    match_sql <- glue::glue("
      SELECT COUNT(*) as n FROM {tmp} s
      INNER JOIN {qname} t ON {join_cond}
    ")
    matches <- DBI::dbGetQuery(con, match_sql)$n[1]

    # Check for duplicates in incoming
    dup_sql <- glue::glue("
      SELECT {by_sql}, COUNT(*) as n FROM {tmp}
      GROUP BY {by_sql}
      HAVING COUNT(*) > 1
    ")
    dups <- DBI::dbGetQuery(con, dup_sql)

    preview$impact <- list(
      inserts = nrow(data) - matches,
      updates = matches,
      duplicates_in_incoming = nrow(dups)
    )

    if (nrow(dups) > 0) {
      preview$impact$duplicate_keys <- head(dups, 5)
    }
  }

  # Print summary
  .print_upsert_preview(preview)

  invisible(preview)
}


#' Print upsert preview
#' @noRd
.print_upsert_preview <- function(preview) {
  cat("\n")
  cat("===================================================================\n")
  cat("  UPSERT PREVIEW\n")
  cat("===================================================================\n\n")

  cat("Target: ", preview$qname, "\n", sep = "")
  cat("Keys:   ", paste(preview$by, collapse = ", "), "\n", sep = "")
  cat("Exists: ", if (preview$table_exists) "Yes" else "No", "\n\n", sep = "")

  # Incoming
  cat("---- Incoming Data ----------------------------------------------------------------------------------------------------\n")
  cat("Rows: ", format(preview$incoming$rows, big.mark = ","), "\n\n", sep = "")

  # Existing
  if (!is.null(preview$existing)) {
    cat("---- Existing Data ----------------------------------------------------------------------------------------------------\n")
    cat("Rows: ", format(preview$existing$rows, big.mark = ","), "\n\n", sep = "")
  }

  # Impact
  cat("---- Impact ------------------------------------------------------------------------------------------------------------------\n")
  cat("Rows to INSERT: ", format(preview$impact$inserts, big.mark = ","), "\n", sep = "")
  cat("Rows to UPDATE: ", format(preview$impact$updates, big.mark = ","), "\n", sep = "")

  if (!is.null(preview$impact$duplicates_in_incoming) && preview$impact$duplicates_in_incoming > 0) {
    cat("\n! WARNING: ", preview$impact$duplicates_in_incoming,
        " duplicate key(s) found in incoming data!\n", sep = "")
    cat("  Use strict=TRUE in db_upsert() to catch this.\n")
    if (!is.null(preview$impact$duplicate_keys)) {
      cat("  First duplicates:\n")
      print(preview$impact$duplicate_keys, row.names = FALSE)
    }
  }

  if (!is.null(preview$update_cols)) {
    if (length(preview$update_cols) == 0) {
      cat("\nMode: INSERT-ONLY (update_cols = character(0))\n")
    } else {
      cat("\nColumns to update: ", paste(preview$update_cols, collapse = ", "), "\n", sep = "")
    }
  }

  cat("\n")
  cat("===================================================================\n")
  cat("Run db_upsert() with same parameters to execute.\n")
  cat("===================================================================\n\n")
}
