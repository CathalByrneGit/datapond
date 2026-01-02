# R/preview.R
# Preview write operations before executing

#' Preview a hive write operation
#'
#' @description Shows what would happen if you ran `db_hive_write()` without
#' actually writing any data. Useful for validating writes before execution.
#'
#' @inheritParams db_hive_write
#' @return A list with preview information (invisibly), also prints a summary
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Preview before writing
#' db_preview_hive_write(my_data, "Trade", "Imports", partition_by = "year")
#'
#' # If preview looks good, actually write
#' db_hive_write(my_data, "Trade", "Imports", partition_by = "year")
#' }
#' @export
#' @importFrom stats setNames
db_preview_hive_write <- function(data,
                                   section,
                                   dataset,
                                   partition_by = NULL,
                                   mode = c("overwrite", "append", "ignore", "replace_partitions")) {

 mode <- match.arg(mode)

  if (!is.data.frame(data)) {
    stop("data must be a data.frame / tibble.", call. = FALSE)
  }

  section <- .db_validate_name(section, "section")
  dataset <- .db_validate_name(dataset, "dataset")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "hive") {
    stop("Connected in DuckLake mode. Use db_preview_lake_write() instead.", call. = FALSE)
  }

  base_path <- .db_get("data_path")
  output_path <- file.path(base_path, section, dataset)

  # Gather preview info
  preview <- list(
    mode = mode,
    section = section,
    dataset = dataset,
    output_path = output_path,
    target_exists = dir.exists(output_path),
    incoming = list(
      rows = nrow(data),
      cols = ncol(data),
      columns = names(data),
      types = vapply(data, function(x) class(x)[1], character(1))
    ),
    existing = NULL,
    schema_changes = NULL,
    partition_impact = NULL
  )

  # Check existing data
  if (preview$target_exists) {
    existing_info <- tryCatch({
      norm_path <- gsub("\\\\", "/", output_path)
      glob_expr <- glue::glue("['{norm_path}/*.parquet', '{norm_path}/**/*.parquet']")

      # Get schema
      schema_sql <- glue::glue("DESCRIBE SELECT * FROM read_parquet({glob_expr}, union_by_name=true)")
      schema_df <- DBI::dbGetQuery(con, schema_sql)
      names(schema_df) <- tolower(names(schema_df))
      if ("name" %in% names(schema_df)) names(schema_df)[names(schema_df) == "name"] <- "column_name"
      if ("type" %in% names(schema_df)) names(schema_df)[names(schema_df) == "type"] <- "column_type"

      # Get row count
      count_sql <- glue::glue("SELECT COUNT(*) as n FROM read_parquet({glob_expr}, union_by_name=true)")
      count_df <- DBI::dbGetQuery(con, count_sql)

      list(
        rows = count_df$n[1],
        cols = nrow(schema_df),
        columns = schema_df$column_name,
        types = setNames(schema_df$column_type, schema_df$column_name)
      )
    }, error = function(e) NULL)

    preview$existing <- existing_info

    # Schema comparison
    if (!is.null(existing_info)) {
      new_cols <- setdiff(preview$incoming$columns, existing_info$columns)
      removed_cols <- setdiff(existing_info$columns, preview$incoming$columns)
      common_cols <- intersect(preview$incoming$columns, existing_info$columns)

      # Check type changes
      type_changes <- list()
      for (col in common_cols) {
        old_type <- existing_info$types[[col]]
        new_type <- preview$incoming$types[[col]]
        # Map R types to DuckDB-ish names for comparison
        new_type_duck <- switch(new_type,
          integer = "INTEGER",
          numeric = "DOUBLE",
          double = "DOUBLE",
          character = "VARCHAR",
          factor = "VARCHAR",
          logical = "BOOLEAN",
          Date = "DATE",
          POSIXct = "TIMESTAMP",
          POSIXlt = "TIMESTAMP",
          new_type
        )
        if (!is.null(old_type) && !grepl(new_type_duck, old_type, ignore.case = TRUE)) {
          type_changes[[col]] <- list(from = old_type, to = new_type)
        }
      }

      preview$schema_changes <- list(
        new_columns = new_cols,
        removed_columns = removed_cols,
        type_changes = type_changes
      )
    }
  }

  # Partition impact
  if (!is.null(partition_by)) {
    part_vals <- unique(data[partition_by])
    preview$partition_impact <- list(
      partition_by = partition_by,
      partitions_in_data = nrow(part_vals),
      partition_values = part_vals
    )

    if (mode == "replace_partitions" && preview$target_exists) {
      # Check which partition folders exist
      to_check <- vapply(seq_len(nrow(part_vals)), function(i) {
        row <- part_vals[i, , drop = FALSE]
        parts <- vapply(partition_by, function(col) {
          val <- row[[col]]
          if (is.numeric(val) && !is.integer(val)) {
            val_str <- sprintf("%.1f", val)
          } else {
            val_str <- as.character(val)
          }
          paste0(col, "=", val_str)
        }, character(1))
        file.path(output_path, paste(parts, collapse = .Platform$file.sep))
      }, character(1))

      existing_parts <- to_check[dir.exists(to_check)]
      preview$partition_impact$existing_partitions_to_replace <- length(existing_parts)
      preview$partition_impact$folders_to_delete <- existing_parts
    }
  }

  # Print summary
  .print_hive_preview(preview)

  invisible(preview)
}


#' Print hive write preview
#' @noRd
#' @importFrom utils head
.print_hive_preview <- function(preview) {
  cat("\n")
  cat("===================================================================\n")
  cat("  WRITE PREVIEW - ", toupper(preview$mode), " MODE\n", sep = "")
  cat("===================================================================\n\n")

  cat("Target: ", preview$section, "/", preview$dataset, "\n", sep = "")
  cat("Path:   ", preview$output_path, "\n", sep = "")
  cat("Exists: ", if (preview$target_exists) "Yes" else "No (will be created)", "\n\n", sep = "")

  # Incoming data
  cat("---- Incoming Data ----------------------------------------------------------------------------------------------------\n")
  cat("Rows:    ", format(preview$incoming$rows, big.mark = ","), "\n", sep = "")
  cat("Columns: ", preview$incoming$cols, "\n", sep = "")
  cat("Schema:  ", paste(names(preview$incoming$types), preview$incoming$types, sep = ":", collapse = ", "), "\n\n", sep = "")

  # Existing data
  if (!is.null(preview$existing)) {
    cat("---- Existing Data ----------------------------------------------------------------------------------------------------\n")
    cat("Rows:    ", format(preview$existing$rows, big.mark = ","), "\n", sep = "")
    cat("Columns: ", preview$existing$cols, "\n\n", sep = "")
  }

  # Schema changes
  if (!is.null(preview$schema_changes)) {
    changes <- preview$schema_changes
    has_changes <- length(changes$new_columns) > 0 ||
                   length(changes$removed_columns) > 0 ||
                   length(changes$type_changes) > 0

    if (has_changes) {
      cat("---- Schema Changes --------------------------------------------------------------------------------------------------\n")
      if (length(changes$new_columns) > 0) {
        cat("  + New columns:     ", paste(changes$new_columns, collapse = ", "), "\n", sep = "")
      }
      if (length(changes$removed_columns) > 0) {
        cat("  - Removed columns: ", paste(changes$removed_columns, collapse = ", "), "\n", sep = "")
      }
      if (length(changes$type_changes) > 0) {
        cat("  ~ Type changes:\n")
        for (col in names(changes$type_changes)) {
          tc <- changes$type_changes[[col]]
          cat("      ", col, ": ", tc$from, " -> ", tc$to, "\n", sep = "")
        }
      }
      cat("\n")
    }
  }

  # Partition impact
  if (!is.null(preview$partition_impact)) {
    cat("---- Partition Impact ----------------------------------------------------------------------------------------------\n")
    cat("Partition by:     ", paste(preview$partition_impact$partition_by, collapse = ", "), "\n", sep = "")
    cat("Partitions:       ", preview$partition_impact$partitions_in_data, "\n", sep = "")

    if (!is.null(preview$partition_impact$existing_partitions_to_replace)) {
      cat("Will replace:     ", preview$partition_impact$existing_partitions_to_replace, " existing partition(s)\n", sep = "")
    }

    if (nrow(preview$partition_impact$partition_values) <= 10) {
      cat("\nPartition values:\n")
      print(preview$partition_impact$partition_values, row.names = FALSE)
    } else {
      cat("\nFirst 10 partition values:\n")
      print(head(preview$partition_impact$partition_values, 10), row.names = FALSE)
      cat("... and ", nrow(preview$partition_impact$partition_values) - 10, " more\n", sep = "")
    }
    cat("\n")
  }

  # Action summary
  cat("---- Action ------------------------------------------------------------------------------------------------------------------\n")
  action <- switch(preview$mode,
    overwrite = if (preview$target_exists) {
      paste0("Will REPLACE ", format(preview$existing$rows, big.mark = ","),
             " existing rows with ", format(preview$incoming$rows, big.mark = ","), " new rows")
    } else {
      paste0("Will CREATE new dataset with ", format(preview$incoming$rows, big.mark = ","), " rows")
    },
    append = paste0("Will ADD ", format(preview$incoming$rows, big.mark = ","), " rows to existing ",
                    format(preview$existing$rows %||% 0, big.mark = ","), " rows"),
    ignore = if (preview$target_exists) "Will SKIP (target already exists)" else
             paste0("Will CREATE new dataset with ", format(preview$incoming$rows, big.mark = ","), " rows"),
    replace_partitions = paste0("Will REPLACE ",
                                preview$partition_impact$existing_partitions_to_replace %||% 0,
                                " partition(s) with ", format(preview$incoming$rows, big.mark = ","), " rows")
  )
  cat(action, "\n\n")

  cat("===================================================================\n")
  cat("Run db_hive_write() with same parameters to execute.\n")
  cat("===================================================================\n\n")
}


#' Preview a DuckLake write operation
#'
#' @description Shows what would happen if you ran `db_lake_write()` without
#' actually writing any data.
#'
#' @inheritParams db_lake_write
#' @return A list with preview information (invisibly), also prints a summary
#'
#' @examples
#' \dontrun{
#' db_lake_connect()
#'
#' db_preview_lake_write(my_data, table = "products", mode = "overwrite")
#' }
#' @export
#' @importFrom stats setNames
db_preview_lake_write <- function(data,
                                   schema = "main",
                                   table,
                                   mode = c("overwrite", "append")) {

  mode <- match.arg(mode)

  if (!is.data.frame(data)) {
    stop("data must be a data.frame / tibble.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Use db_preview_hive_write() instead.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  qname <- paste0(catalog, ".", schema, ".", table)

  # Check if table exists
  table_exists <- .db_table_exists(con, catalog, schema, table)

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
    schema_changes = NULL
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
  .print_lake_preview(preview)

  invisible(preview)
}


#' Print DuckLake write preview
#' @noRd
.print_lake_preview <- function(preview) {
  cat("\n")
  cat("===================================================================\n")
  cat("  DUCKLAKE WRITE PREVIEW - ", toupper(preview$mode), " MODE\n", sep = "")
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
  cat("Run db_lake_write() with same parameters to execute.\n")
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
#' db_lake_connect()
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
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("db_preview_upsert is only available in DuckLake mode.", call. = FALSE)
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
