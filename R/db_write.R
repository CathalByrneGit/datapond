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
#' @description
#' Writes data to a DuckLake table. Supports two input types:
#'
#' **data.frame/tibble**: Data is transferred from R to DuckDB. Use this when
#' you have data in R memory (e.g., from CSV, API, or computation).
#'
#' **Lazy dbplyr table**: Data stays in DuckDB - no R memory used. Use this for

#' transformations within the lake (e.g., cleaning, aggregating). The dplyr
#' pipeline is converted to SQL and executed as `CREATE TABLE AS SELECT` or
#' `INSERT INTO ... SELECT`.
#'
#' @param data A data.frame, tibble, or lazy dbplyr table (from `db_read()` or
#'   `tbl()`). Lazy tables enable zero-copy transformations within DuckDB.
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param mode "overwrite" or "append"
#' @param col_types Optional named list or character vector specifying column types.
#'   Only applies to data.frame input (ignored for lazy tables).
#'   Overrides automatic type inference for stricter schema control.
#'   Format: `list(id = "BIGINT", value = "DECIMAL(10,2)")` or
#'   `c(id = "BIGINT", value = "DECIMAL(10,2)")`.
#'   Supported types: INTEGER, BIGINT, DOUBLE, DECIMAL(p,s), VARCHAR, BOOLEAN,
#'   DATE, TIMESTAMP, INTERVAL, BLOB, GEOMETRY, etc.
#' @param partition_by Optional character vector of column names to partition by.
#'   Only valid for mode = "overwrite". On overwrite, if not specified, existing
#'   partitioning is preserved.
#' @param bucket_by Optional list specifying bucket partitioning for high-cardinality
#'   columns. Format: `list(column = "col_name", buckets = 16)`. Uses Iceberg-compatible
#'   Murmur3 hashing. Only valid for mode = "overwrite".
#' @param sort_by Optional character vector of column names to sort/cluster by.
#'   Improves query performance for range scans and filters on these columns.
#'   Only valid for mode = "overwrite".
#' @param inline Deprecated. DuckLake automatically inlines small writes based on
#'   the `data_inlining_row_limit` threshold (default 10 rows). Use
#'   [db_set_inline_threshold()] to adjust the threshold for a table.
#' @param commit_author Optional author for DuckLake commit metadata
#' @param commit_message Optional message for DuckLake commit metadata
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' # ==== data.frame approach (data passes through R) ====
#'
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
#' # With explicit column types for stricter schema control
#' db_write(my_data, table = "financials",
#'          col_types = list(id = "BIGINT", amount = "DECIMAL(12,2)"))
#'
#' # ==== Lazy table approach (zero-copy, stays in DuckDB) ====
#'
#' # Transform and write without collect() - no R memory used
#' db_read(table = "raw_imports") |>
#'   filter(year == 2024) |>
#'   mutate(value_eur = value * exchange_rate) |>
#'   group_by(country, month) |>
#'   summarise(total = sum(value_eur), .groups = "drop") |>
#'   db_write(schema = "clean", table = "monthly_summary")
#'
#' # Append transformed data
#' db_read(table = "staging") |>
#'   filter(!is.na(id)) |>
#'   db_write(table = "production", mode = "append")
#'
#' # Join tables and write result
#' orders <- db_read(table = "orders")
#' products <- db_read(table = "products")
#'
#' orders |>
#'   left_join(products, by = "product_id") |>
#'   select(order_id, product_name, quantity, price) |>
#'   db_write(table = "order_details")
#'
#' # ==== Other options ====
#'
#' # DuckLake automatically inlines small writes (< threshold rows)
#' # Use db_set_inline_threshold() to adjust the threshold
#' db_write(batch, table = "events", mode = "append")
#'
#' # With commit metadata
#' db_write(my_data, table = "imports", mode = "append",
#'          commit_author = "jsmith",
#'          commit_message = "Added Q3 data")
#' }
#' @seealso [db_flush_inlined()] to flush inlined data, [db_set_clustering()] to
#'   change clustering on existing tables, [db_recluster()] to re-sort data
#' @export
db_write <- function(data,
                     schema = "main",
                     table,
                     mode = c("overwrite", "append"),
                     col_types = NULL,
                     partition_by = NULL,
                     bucket_by = NULL,
                     sort_by = NULL,
                     inline = FALSE,
                     commit_author = NULL,
                     commit_message = NULL) {

  mode <- match.arg(mode)

  # Determine if data is a lazy dbplyr table or a data.frame
 is_lazy <- inherits(data, "tbl_lazy") || inherits(data, "tbl_sql")

  if (!is_lazy && !is.data.frame(data)) {
    stop("data must be a data.frame, tibble, or lazy dbplyr table.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")

  # For lazy tables, col_types is not supported (types come from the query)
  if (is_lazy && !is.null(col_types)) {
    warning("col_types is ignored for lazy tables - column types are determined by the query.",
            call. = FALSE)
    col_types <- NULL
  }

  # Get column names (works for both data.frame and lazy tables)
  data_cols <- if (is_lazy) colnames(data) else names(data)

  # Validate partition_by
  if (!is.null(partition_by)) {
    if (!is.character(partition_by) || length(partition_by) == 0) {
      stop("partition_by must be a non-empty character vector.", call. = FALSE)
    }
    missing_cols <- setdiff(partition_by, data_cols)
    if (length(missing_cols) > 0) {
      stop("partition_by columns not found in data: ",
           paste(missing_cols, collapse = ", "),
           call. = FALSE)
    }
  }

  # Validate bucket_by
  if (!is.null(bucket_by)) {
    if (!is.list(bucket_by) || is.null(bucket_by$column) || is.null(bucket_by$buckets)) {
      stop("bucket_by must be a list with 'column' and 'buckets' elements.", call. = FALSE)
    }
    if (!bucket_by$column %in% data_cols) {
      stop("bucket_by column '", bucket_by$column, "' not found in data.", call. = FALSE)
    }
    if (!is.numeric(bucket_by$buckets) || bucket_by$buckets < 1) {
      stop("bucket_by$buckets must be a positive integer.", call. = FALSE)
    }
  }

  # Validate sort_by
  if (!is.null(sort_by)) {
    if (!is.character(sort_by) || length(sort_by) == 0) {
      stop("sort_by must be a non-empty character vector.", call. = FALSE)
    }
    missing_cols <- setdiff(sort_by, data_cols)
    if (length(missing_cols) > 0) {
      stop("sort_by columns not found in data: ",
           paste(missing_cols, collapse = ", "),
           call. = FALSE)
    }
  }

  # inline parameter is deprecated - DuckLake auto-inlines based on threshold
  if (isTRUE(inline)) {
    message("Note: 'inline' parameter is deprecated. DuckLake automatically inlines ",
            "small writes based on data_inlining_row_limit threshold (default 10 rows). ",
            "Use db_set_inline_threshold() to adjust.")
  }

  # Validate and normalize col_types (only for data.frame input)
  if (!is.null(col_types) && !is_lazy) {
    if (!is.list(col_types) && !is.character(col_types)) {
      stop("col_types must be a named list or named character vector.", call. = FALSE)
    }
    if (is.null(names(col_types)) || any(names(col_types) == "")) {
      stop("col_types must have names for all elements.", call. = FALSE)
    }
    # Convert to list if character vector
    if (is.character(col_types)) {
      col_types <- as.list(col_types)
    }
    # Check that specified columns exist in data
    unknown_cols <- setdiff(names(col_types), data_cols)
    if (length(unknown_cols) > 0) {
      stop("col_types specifies columns not found in data: ",
           paste(unknown_cols, collapse = ", "),
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
    if (!is.null(bucket_by)) {
      stop("bucket_by cannot be used with mode = 'append'.", call. = FALSE)
    }
    if (!is.null(sort_by)) {
      stop("sort_by cannot be used with mode = 'append'. Use db_set_clustering() to change sort order.", call. = FALSE)
    }
  }

  # Preserve existing partitioning on overwrite if not explicitly specified
  if (mode == "overwrite" && table_exists && is.null(partition_by)) {
    existing_parts <- .db_get_partitioning_internal(con, catalog, schema, table)
    if (!is.null(existing_parts) && length(existing_parts) > 0) {
      partition_by <- existing_parts
    }
  }

  # Helper to set partitioning and clustering after table creation
  .apply_table_options <- function() {
    # Set partitioning if specified
    partition_parts <- c()
    if (!is.null(partition_by)) {
      partition_parts <- c(partition_parts, partition_by)
    }
    if (!is.null(bucket_by)) {
      bucket_clause <- paste0("bucket(", as.integer(bucket_by$buckets), ", ", bucket_by$column, ")")
      partition_parts <- c(partition_parts, bucket_clause)
    }
    if (length(partition_parts) > 0) {
      partition_clause <- paste(partition_parts, collapse = ", ")
      DBI::dbExecute(con, glue::glue("ALTER TABLE {qname} SET PARTITIONED BY ({partition_clause})"))
    }

    # Set clustering/sorting if specified
    if (!is.null(sort_by)) {
      sort_clause <- paste(sort_by, collapse = ", ")
      DBI::dbExecute(con, glue::glue("ALTER TABLE {qname} SET SORTED BY ({sort_clause})"))
    }
  }

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
    if (is_lazy) {
      # ========== LAZY TABLE PATH ==========
      # Extract SQL from the dbplyr query - stays entirely in DuckDB
      select_sql <- dbplyr::sql_render(data)

      if (mode == "overwrite") {
        # Drop existing table if present
        if (table_exists) {
          DBI::dbExecute(con, glue::glue("DROP TABLE {qname}"))
        }

        # Create table directly from query
        DBI::dbExecute(con, glue::glue("CREATE TABLE {qname} AS {select_sql}"))

        # Apply partitioning and clustering
        .apply_table_options()

      } else {
        # Append mode (inlining is automatic based on row count threshold)
        DBI::dbExecute(con, glue::glue("INSERT INTO {qname} {select_sql}"))
      }

    } else {
      # ========== DATA.FRAME PATH ==========
      # Register data.frame as temporary view
      tmp <- .db_temp_name()
      duckdb::duckdb_register(con, tmp, data)
      on.exit(try(duckdb::duckdb_unregister(con, tmp), silent = TRUE), add = TRUE)

      if (mode == "overwrite") {
        # Drop existing table if present
        if (table_exists) {
          DBI::dbExecute(con, glue::glue("DROP TABLE {qname}"))
        }

        # Build column definitions from data types
        # Use explicit col_types where specified, otherwise infer from data
        cols <- vapply(data, .db_r_to_duckdb_type, character(1))
        if (!is.null(col_types)) {
          for (col_name in names(col_types)) {
            cols[col_name] <- col_types[[col_name]]
          }
        }
        col_defs <- paste(
          paste(names(cols), cols),
          collapse = ", "
        )

        # Create table with explicit schema
        create_sql <- glue::glue("CREATE TABLE {qname} ({col_defs})")
        DBI::dbExecute(con, create_sql)

        # Apply partitioning and clustering
        .apply_table_options()

        # Insert data
        DBI::dbExecute(con, glue::glue("INSERT INTO {qname} SELECT * FROM {tmp}"))

      } else {
        # Append mode (inlining is automatic based on row count threshold)
        DBI::dbExecute(con, glue::glue("INSERT INTO {qname} SELECT * FROM {tmp}"))
      }
    }

    DBI::dbExecute(con, "COMMIT")

  }, error = function(e) {
    try(DBI::dbExecute(con, "ROLLBACK"), silent = TRUE)
    stop(e$message, call. = FALSE)
  })

  action <- ifelse(mode == "overwrite", "Wrote", "Appended")
  source <- if (is_lazy) "(from query)" else "(from data.frame)"
  message(action, " data to ", qname, " ", source)

  invisible(qname)
}
