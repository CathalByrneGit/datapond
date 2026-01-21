# R/write.R

# internal: create a unique temp view name
.db_temp_name <- function(prefix = "db_tmp_") {
  paste0(prefix, paste0(sample(c(letters, 0:9), 16, replace = TRUE), collapse = ""))
}

# internal: compression validation
.db_validate_compression <- function(compression) {
  if (is.null(compression)) return(NULL)

  if (!is.character(compression) || length(compression) != 1L || is.na(compression) || !nzchar(compression)) {
    stop("compression must be NULL or a non-empty single string.", call. = FALSE)
  }

  comp <- tolower(trimws(compression))

  # Keep this conservative; expand if your DuckDB build supports more codecs.
  known <- c("zstd", "snappy", "gzip", "brotli", "lz4", "lz4_raw", "uncompressed")

  if (!comp %in% known) {
    stop(
      "Unsupported compression: '", compression, "'. ",
      "Allowed: ", paste(known, collapse = ", "),
      call. = FALSE
    )
  }

  comp
}

# internal: optional governance hook
# If you store a named list in .db_env$partition_rules, enforce it:
# e.g. set_partition_rules(list("Trade/Imports" = c("year","month")))
.db_enforce_partition_governance <- function(section, dataset, partition_by) {
  rules <- .db_get("partition_rules")
  if (is.null(rules) || !is.list(rules)) return(invisible(TRUE))

  key <- paste0(section, "/", dataset)
  if (!key %in% names(rules)) return(invisible(TRUE)) # no rule defined => allow

  required <- rules[[key]]
  if (is.null(partition_by)) {
    stop("Partition governance: dataset '", key, "' requires partition_by = ",
         paste(required, collapse = ", "), call. = FALSE)
  }

  if (!identical(partition_by, required)) {
    stop(
      "Partition governance: dataset '", key, "' requires partition_by = ",
      paste(required, collapse = ", "), ". You supplied: ",
      paste(partition_by, collapse = ", "),
      call. = FALSE
    )
  }

  invisible(TRUE)
}


#' Publish / Append / Ignore / Replace Partitions in the Hive Lake
#' 
#' @param data A data.frame / tibble
#' @param section Your section name
#' @param dataset The name of the dataset
#' @param partition_by Character vector of column names to partition by (e.g. c("year","month"))
#' @param mode One of:
#'   - "overwrite": replace target files
#'   - "append": add new files (requires unique filenames)
#'   - "ignore": write only if target path does not exist (best-effort; still race-prone)
#'   - "replace_partitions": delete only affected partition folders, then append fresh files (requires partition_by)
#' @param compression Parquet compression codec (NULL means DuckDB default). 
#'   Options: "zstd", "snappy", "gzip", "brotli", "lz4", "lz4_raw", "uncompressed"
#' @param filename_pattern Used in append-like modes (default "data_{uuid}")
#' @return Invisibly returns the output path
#' @examples
#' \dontrun{
#' # Basic overwrite
#' db_hive_write(my_data, "Trade", "Imports")
#' 
#' # Partitioned write
#' db_hive_write(my_data, "Trade", "Imports", partition_by = c("year", "month"))
#' 
#' # Append mode
#' db_hive_write(my_data, "Trade", "Imports", mode = "append")
#' 
#' # Replace only touched partitions
#' db_hive_write(my_data, "Trade", "Imports", 
#'               partition_by = c("year", "month"), 
#'               mode = "replace_partitions")
#' }
#' @export
db_hive_write <- function(data,
                          section,
                          dataset,
                          partition_by = NULL,
                          mode = c("overwrite", "append", "ignore", "replace_partitions"),
                          compression = NULL,
                          filename_pattern = "data_{uuid}") {

  mode <- match.arg(mode, c("overwrite", "append", "ignore", "replace_partitions"))

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
    stop("Connected in DuckLake mode. Use db_lake_write() instead, or reconnect with db_connect().", call. = FALSE)
  }

  # Validate partition_by
  if (!is.null(partition_by)) {
    if (!is.character(partition_by) || length(partition_by) < 1L) {
      stop("partition_by must be NULL or a character vector.", call. = FALSE)
    }
    if (anyNA(partition_by) || any(!nzchar(partition_by))) {
      stop("partition_by cannot contain NA/empty strings.", call. = FALSE)
    }
    missing_cols <- setdiff(partition_by, names(data))
    if (length(missing_cols) > 0) {
      stop("partition_by columns not found in data: ",
           paste(missing_cols, collapse = ", "),
           call. = FALSE)
    }
  } else if (identical(mode, "replace_partitions")) {
    stop("mode = 'replace_partitions' requires partition_by (so we know what to replace).",
         call. = FALSE)
  } else if (identical(mode, "append")) {
    stop("mode = 'append' requires partition_by. DuckDB cannot append to a directory without partitioning.",
         call. = FALSE)
  }

  # Optional governance enforcement
  .db_enforce_partition_governance(section, dataset, partition_by)

  # Validate compression
  compression <- .db_validate_compression(compression)

  base_path <- .db_get("data_path")
  if (is.null(base_path)) {
    stop("No data path configured.", call. = FALSE)
  }
  
  output_path <- file.path(base_path, section, dataset)

  # Ensure output directory exists
  if (!dir.exists(output_path)) {
    dir.create(output_path, recursive = TRUE)
  }

  # Determine write path based on partitioning
  # - Partitioned writes: write to directory (DuckDB creates partition folders)
  # - Non-partitioned writes: write to a file within the directory
  if (!is.null(partition_by)) {
    write_path <- output_path
  } else {
    # For non-partitioned, use fixed filename for overwrite, pattern for append
    if (identical(mode, "overwrite") || identical(mode, "ignore")) {
      write_path <- file.path(output_path, "data.parquet")
    } else {
      # append mode - write to directory with filename pattern
      write_path <- output_path
    }
  }

  # "ignore" semantics: if the dataset folder already exists, do nothing.
  # NOTE: this is not transactional; concurrent writers can still race.
  if (identical(mode, "ignore") && dir.exists(output_path)) {
    message("Ignored write: target already exists at ", output_path)
    return(invisible(output_path))
  }

  temp_name <- .db_temp_name()
  duckdb::duckdb_register(con, temp_name, data)
  on.exit({
    try(duckdb::duckdb_unregister(con, temp_name), silent = TRUE)
  }, add = TRUE)

  # If replacing partitions, delete only the partition folders touched by 'data'
  if (identical(mode, "replace_partitions")) {
    if (!requireNamespace("fs", quietly = TRUE)) {
      stop("mode = 'replace_partitions' requires the 'fs' package.", call. = FALSE)
    }

    # Find distinct partition combinations from the incoming data
    part_vals <- unique(data[partition_by])

    # Build partition directories like: output_path/year=2024/month=01
    # (Hive folder convention)
    # NOTE: DuckDB formats numeric (double) values with .0 suffix (e.g., year=2024.0)
    #       but integer values without (e.g., year=2024)
    to_delete <- vapply(seq_len(nrow(part_vals)), function(i) {
      row <- part_vals[i, , drop = FALSE]
      parts <- vapply(partition_by, function(col) {
        val <- row[[col]]
        # Format value to match DuckDB's hive folder naming:
        # - integers: "2024"
        # - doubles: "2024.0" (even if whole number)
        if (is.numeric(val) && !is.integer(val)) {
          # Double - DuckDB adds .0 for whole numbers
          val_str <- sprintf("%.1f", val)
          # But if it has more precision, use full representation
          if (abs(val - round(val, 1)) > .Machine$double.eps) {
            val_str <- as.character(val)
          }
        } else {
          val_str <- as.character(val)
        }
        paste0(col, "=", val_str)
      }, character(1))
      file.path(output_path, paste(parts, collapse = .Platform$file.sep))
    }, character(1))

    # Delete any that exist (and only those)
    existing <- to_delete[dir.exists(to_delete)]
    if (length(existing) > 0) {
      fs::dir_delete(existing)
    }
  }

  # Build COPY options
  opts <- c("FORMAT PARQUET")

  # Mode options
  if (identical(mode, "overwrite")) {
    opts <- c(opts, "OVERWRITE")
  } else if (identical(mode, "ignore")) {
    opts <- c(opts, "OVERWRITE_OR_IGNORE")
  } else {
    # append + replace_partitions both want append semantics + unique filenames
    fp <- gsub("'", "''", filename_pattern)
    opts <- c(opts, "APPEND", glue::glue("FILENAME_PATTERN '{fp}'"))
  }

  if (!is.null(partition_by)) {
    opts <- c(opts, glue::glue("PARTITION_BY ({paste(partition_by, collapse = ', ')})"))
  }

  if (!is.null(compression)) {
    comp <- gsub("'", "''", compression)
    opts <- c(opts, glue::glue("COMPRESSION '{comp}'"))
  }

  sql <- glue::glue(
    "COPY {temp_name} TO {.db_sql_quote(write_path)} ({paste(opts, collapse = ', ')})"
  )

  DBI::dbExecute(con, sql)

  msg <- switch(
    mode,
    overwrite = "Published (overwrote) data to",
    append = "Appended data to",
    ignore = "Published (ignored conflicts) data to",
    replace_partitions = "Replaced touched partitions and appended data to"
  )
  message(msg, " ", output_path)

  invisible(output_path)
}


# internal: get partitioning for a table (without connection validation)
.db_get_partitioning_internal <- function(con, catalog, schema, table) {
  metadata_schema <- paste0("__ducklake_metadata_", catalog)

  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT DISTINCT c.column_name, pc.partition_key_index
      FROM {metadata_schema}.ducklake_partition_column pc
      JOIN {metadata_schema}.ducklake_table t ON pc.table_id = t.table_id
      JOIN {metadata_schema}.ducklake_schema s ON t.schema_id = s.schema_id
      JOIN {metadata_schema}.ducklake_column c ON pc.column_id = c.column_id
      WHERE s.schema_name = '{schema}' AND t.table_name = '{table}'
      ORDER BY pc.partition_key_index
    "))
  }, error = function(e) {
    data.frame(column_name = character(0), partition_key_index = integer(0))
  })

  if (nrow(result) == 0) {
    return(NULL)
  }

  # Return column names only (transforms are applied by DuckLake automatically)
  result$column_name
}

# internal: map R column class to DuckDB SQL type
.db_r_to_duckdb_type <- function(x) {
  cls <- class(x)[1]

  switch(cls,
    integer = "INTEGER",
    numeric = "DOUBLE",
    double = "DOUBLE",
    character = "VARCHAR",
    factor = "VARCHAR",
    logical = "BOOLEAN",
    Date = "DATE",
    POSIXct = "TIMESTAMP",
    POSIXlt = "TIMESTAMP",
    difftime = "INTERVAL",
    raw = "BLOB",
    # Default fallback
    "VARCHAR"
  )
}


#' Write a DuckLake table (overwrite/append)
#'
#' @param data data.frame/tibble
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param mode "overwrite" or "append"
#' @param partition_by Optional character vector of column names to partition by.
#'   Only valid for mode = "overwrite". On overwrite, if not specified, existing
#'   partitioning is preserved.
#' @param commit_author Optional author for DuckLake commit metadata
#' @param commit_message Optional message for DuckLake commit metadata
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' # Basic overwrite
#' db_lake_write(my_data, table = "imports")
#'
#' # With schema
#' db_lake_write(my_data, schema = "trade", table = "imports")
#'
#' # With partitioning (overwrite mode only)
#' db_lake_write(my_data, schema = "trade", table = "imports",
#'               partition_by = c("year", "month"))
#'
#' # Append mode with commit info
#' db_lake_write(my_data, table = "imports", mode = "append",
#'               commit_author = "jsmith",
#'               commit_message = "Added Q3 data")
#' }
#' @export
db_lake_write <- function(data,
                          schema = "main",
                          table,
                          mode = c("overwrite", "append"),
                          partition_by = NULL,
                          commit_author = NULL,
                          commit_message = NULL) {

  mode <- match.arg(mode)

  if (!is.data.frame(data)) {
    stop("data must be a data.frame / tibble.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")

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
  }

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Use db_hive_write() instead, or reconnect with db_lake_connect().", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", table)
  table_exists <- .db_table_exists(con, catalog, schema, table)

  # Validate mode-specific constraints
  if (mode == "append") {
    if (!table_exists) {
      stop("Table '", qname, "' does not exist. Use mode = 'overwrite' to create it first.", call. = FALSE)
    }
    if (!is.null(partition_by)) {
      stop("partition_by cannot be used with mode = 'append'. Use db_set_partitioning() to change partitioning.", call. = FALSE)
    }
  }

  # Preserve existing partitioning on overwrite if not explicitly specified
  if (mode == "overwrite" && table_exists && is.null(partition_by)) {
    existing_parts <- .db_get_partitioning_internal(con, catalog, schema, table)
    if (!is.null(existing_parts) && length(existing_parts) > 0) {
      partition_by <- existing_parts
    }
  }

  tmp <- .db_temp_name()
  duckdb::duckdb_register(con, tmp, data)
  on.exit(try(duckdb::duckdb_unregister(con, tmp), silent = TRUE), add = TRUE)

  DBI::dbExecute(con, "BEGIN")

  # Optional: add commit message/author inside txn
  if (!is.null(commit_author) || !is.null(commit_message)) {
    author  <- if (is.null(commit_author)) "NULL" else .db_sql_quote(commit_author)
    msg_val <- if (is.null(commit_message)) "NULL" else .db_sql_quote(commit_message)
    DBI::dbExecute(con, glue::glue(
      "CALL ducklake_set_commit_message({.db_sql_quote(catalog)}, {author}, {msg_val})"
    ))
  }

  tryCatch({
    if (mode == "overwrite") {
      # Drop existing table if present
      if (table_exists) {
        DBI::dbExecute(con, glue::glue("DROP TABLE {qname}"))
      }

      # Build column definitions from data types
      cols <- vapply(data, .db_r_to_duckdb_type, character(1))
      col_defs <- paste(
        paste(names(cols), cols),
        collapse = ", "
      )

      # Create table with explicit schema
      create_sql <- glue::glue("CREATE TABLE {qname} ({col_defs})")
      DBI::dbExecute(con, create_sql)

      # Set partitioning if specified (must be done before inserting data)
      if (!is.null(partition_by)) {
        partition_clause <- paste(partition_by, collapse = ", ")
        DBI::dbExecute(con, glue::glue("ALTER TABLE {qname} SET PARTITIONED BY ({partition_clause})"))
      }

      # Insert data
      DBI::dbExecute(con, glue::glue("INSERT INTO {qname} SELECT * FROM {tmp}"))

    } else {
      # append mode - just insert
      DBI::dbExecute(con, glue::glue("INSERT INTO {qname} SELECT * FROM {tmp}"))
    }

    DBI::dbExecute(con, "COMMIT")

  }, error = function(e) {
    try(DBI::dbExecute(con, "ROLLBACK"), silent = TRUE)
    stop(e$message, call. = FALSE)
  })

  message(ifelse(mode == "overwrite", "Wrote", "Appended"), " data to ", qname)

  invisible(qname)
}
