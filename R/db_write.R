# R/db_write.R

# internal: create a unique temp view name
.db_temp_name <- function(prefix = "db_tmp_") {
  paste0(prefix, paste0(sample(c(letters, 0:9), 16, replace = TRUE), collapse = ""))
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
    JOIN {metadata_schema}.ducklake_column c
      ON pc.column_id = c.column_id
     AND c.table_id  = t.table_id
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
#' db_write(my_data, table = "imports")
#'
#' # With schema
#' db_write(my_data, schema = "trade", table = "imports")
#'
#' # With partitioning (overwrite mode only)
#' db_write(my_data, schema = "trade", table = "imports",
#'          partition_by = c("year", "month"))
#'
#' # Append mode with commit info
#' db_write(my_data, table = "imports", mode = "append",
#'          commit_author = "jsmith",
#'          commit_message = "Added Q3 data")
#' }
#' @export
db_write <- function(data,
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
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
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
