# R/upsert.R

# ---- Column helpers ----

# internal: fetch columns from information_schema for a given table/view
.db_relation_cols <- function(con, catalog, schema, name) {
  catalog <- .db_validate_name(catalog, "catalog")
  schema  <- .db_validate_name(schema, "schema")
  name    <- .db_validate_name(name, "name")

  DBI::dbGetQuery(
    con,
    glue::glue(
      "
      SELECT column_name
      FROM information_schema.columns
      WHERE table_catalog = {.db_sql_quote(catalog)}
        AND table_schema  = {.db_sql_quote(schema)}
        AND table_name    = {.db_sql_quote(name)}
      ORDER BY ordinal_position
      "
    )
  )$column_name
}

#' Get column names for a DuckLake table
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return Character vector of column names
#' @export
db_table_cols <- function(schema = "main", table) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  .db_relation_cols(con, catalog, schema, table)
}

#' Get column names for a DuckLake view
#'
#' @param schema Schema name (default "main")
#' @param view View name
#' @return Character vector of column names
#' @export
db_view_cols <- function(schema = "main", view) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  .db_relation_cols(con, catalog, schema, view)
}

# internal: check if a table exists
.db_table_exists <- function(con, catalog, schema, table) {
  catalog <- .db_validate_name(catalog, "catalog")
  schema  <- .db_validate_name(schema, "schema")
  table   <- .db_validate_name(table, "table")

  n <- DBI::dbGetQuery(
    con,
    glue::glue(
      "
      SELECT COUNT(*) AS n
      FROM information_schema.tables
      WHERE table_catalog = {.db_sql_quote(catalog)}
        AND table_schema  = {.db_sql_quote(schema)}
        AND table_name    = {.db_sql_quote(table)}
      "
    )
  )$n[[1]]

  isTRUE(n > 0)
}

# ---- Upsert ----

#' Upsert into a DuckLake table using MERGE INTO
#'
#' @param data data.frame / tibble
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param by Character vector of key columns used to match rows
#' @param strict If TRUE (default), refuse to upsert if duplicates exist in `data` for the `by` key.
#' @param update_cols Controls which columns to update on match:
#'   - NULL (default): update all columns
#'   - character(0): insert-only (no updates on match)
#'   - character vector: update only specified columns
#' @param commit_author Optional DuckLake commit author
#' @param commit_message Optional DuckLake commit message
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' # Basic upsert by id
#' db_upsert(my_data, table = "products", by = "product_id")
#'
#' # Composite key
#' db_upsert(my_data, table = "sales", by = c("region", "date"))
#'
#' # Update only specific columns
#' db_upsert(my_data, table = "products", by = "product_id",
#'           update_cols = c("price", "updated_at"))
#'
#' # Insert-only (no updates)
#' db_upsert(my_data, table = "events", by = "event_id",
#'           update_cols = character(0))
#'
#' # With commit metadata
#' db_upsert(my_data, table = "products", by = "product_id",
#'           commit_author = "jsmith",
#'           commit_message = "Price update batch")
#' }
#' @export
db_upsert <- function(data,
                      schema = "main",
                      table,
                      by,
                      strict = TRUE,
                      update_cols = NULL,
                      commit_author = NULL,
                      commit_message = NULL) {

  if (!is.data.frame(data)) {
    stop("`data` must be a data.frame / tibble.", call. = FALSE)
  }

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

  qname <- glue::glue("{catalog}.{schema}.{table}")

  if (!is.character(by) || length(by) < 1 || any(!nzchar(by))) {
    stop("`by` must be a non-empty character vector of key columns.", call. = FALSE)
  }

  # data-side key validation
  missing_keys <- setdiff(by, names(data))
  if (length(missing_keys) > 0) {
    stop("Key columns not found in data: ", paste(missing_keys, collapse = ", "), call. = FALSE)
  }

  # table existence + column introspection
  if (!.db_table_exists(con, catalog, schema, table)) {
    stop("Target table does not exist: ", qname, call. = FALSE)
  }

  target_cols <- .db_relation_cols(con, catalog, schema, table)
  data_cols   <- names(data)

  # Basic column governance for upsert:
  # - data must not contain columns that target table doesn't have
  extra_in_data <- setdiff(data_cols, target_cols)
  if (length(extra_in_data) > 0) {
    stop(
      "Data has columns not present in target table ", qname, ": ",
      paste(extra_in_data, collapse = ", "),
      call. = FALSE
    )
  }

  # Ensure keys exist in target
  missing_keys_in_target <- setdiff(by, target_cols)
  if (length(missing_keys_in_target) > 0) {
    stop(
      "Key columns not present in target table ", qname, ": ",
      paste(missing_keys_in_target, collapse = ", "),
      call. = FALSE
    )
  }

  # update_cols semantics
  # - NULL => "update all" shorthand (WHEN MATCHED THEN UPDATE)
  # - character(0) => insert-only (no matched clause)
  # - otherwise => UPDATE SET col = s.col ...
  if (!is.null(update_cols)) {
    if (!is.character(update_cols) || any(!nzchar(update_cols))) {
      stop("`update_cols` must be NULL, character(0), or a character vector.", call. = FALSE)
    }
    if (length(update_cols) > 0) {
      missing_upd <- setdiff(update_cols, data_cols)
      if (length(missing_upd) > 0) {
        stop("update_cols not found in data: ", paste(missing_upd, collapse = ", "), call. = FALSE)
      }
    }
  }

  # Register data as temporary view
  tmp <- .db_temp_name()
  duckdb::duckdb_register(con, tmp, data)
  on.exit(try(duckdb::duckdb_unregister(con, tmp), silent = TRUE), add = TRUE)

  # ---- STRICT MODE: reject duplicate keys in incoming data ----
  if (isTRUE(strict)) {
    by_sql <- paste(by, collapse = ", ")
    dup_n <- DBI::dbGetQuery(con, glue::glue("
      SELECT COUNT(*) AS n_dups
      FROM (
        SELECT {by_sql}
        FROM {tmp}
        GROUP BY {by_sql}
        HAVING COUNT(*) > 1
      )
    "))$n_dups[[1]]

    if (dup_n > 0) {
      sample_dups <- DBI::dbGetQuery(con, glue::glue("
        SELECT {by_sql}, COUNT(*) AS n
        FROM {tmp}
        GROUP BY {by_sql}
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        LIMIT 20
      "))
      stop(
        "strict=TRUE: incoming data contains duplicate keys for `by` (", paste(by, collapse = ", "), ").\n",
        "Found ", dup_n, " duplicated key group(s). Showing up to 20:\n",
        paste(utils::capture.output(print(sample_dups, row.names = FALSE)), collapse = "\n"),
        call. = FALSE
      )
    }
  }

  # Build ON clause
  on_sql <- paste(glue::glue("t.{by} = s.{by}"), collapse = " AND ")

  # Decide INSERT form:
  # - If data columns are exactly the same set as target columns, we can safely use bare INSERT.
  # - Otherwise, use INSERT (col1,...) VALUES (s.col1,...).
  can_bare_insert <- setequal(data_cols, target_cols)

  insert_clause <- if (can_bare_insert) {
    "WHEN NOT MATCHED THEN INSERT"
  } else {
    insert_sql_cols <- paste(data_cols, collapse = ", ")
    insert_sql_vals <- paste(glue::glue("s.{data_cols}"), collapse = ", ")
    glue::glue("WHEN NOT MATCHED THEN INSERT ({insert_sql_cols}) VALUES ({insert_sql_vals})")
  }

  # Build UPDATE clause based on update_cols:
  if (is.null(update_cols)) {
    # DuckDB shorthand: update all columns by name
    update_clause <- "WHEN MATCHED THEN UPDATE"
  } else if (length(update_cols) == 0) {
    # insert-only: no matched clause
    update_clause <- ""
  } else {
    update_sql <- paste(glue::glue("{update_cols} = s.{update_cols}"), collapse = ", ")
    update_clause <- glue::glue("WHEN MATCHED THEN UPDATE SET {update_sql}")
  }

  # Assemble MERGE
  merge_parts <- c(
    glue::glue("MERGE INTO {qname} AS t"),
    glue::glue("USING {tmp} AS s"),
    glue::glue("ON ({on_sql})")
  )

  if (nzchar(update_clause)) {
    merge_parts <- c(merge_parts, update_clause)
  }
  merge_parts <- c(merge_parts, insert_clause)

  merge_sql <- paste(merge_parts, collapse = "\n")

  # Transaction + optional DuckLake commit metadata
  DBI::dbExecute(con, "BEGIN")

  if (!is.null(commit_author) || !is.null(commit_message)) {
    author_val <- if (is.null(commit_author)) "NULL" else .db_sql_quote(commit_author)
    msg_val    <- if (is.null(commit_message)) "NULL" else .db_sql_quote(commit_message)
    DBI::dbExecute(con, glue::glue("CALL ducklake_set_commit_message({.db_sql_quote(catalog)}, {author_val}, {msg_val})"))
  }

  tryCatch({
    DBI::dbExecute(con, merge_sql)
    DBI::dbExecute(con, "COMMIT")
  }, error = function(e) {
    try(DBI::dbExecute(con, "ROLLBACK"), silent = TRUE)
    stop(e$message, call. = FALSE)
  })

  message("Upserted data into ", qname)

  invisible(as.character(qname))
}
