# R/discovery.R

#' List schemas in the DuckLake catalog
#'
#' @description Returns all schemas in the connected DuckLake catalog.
#' @return Character vector of schema names
#' @examples
#' \dontrun{
#' db_connect()
#' db_list_schemas()
#' # [1] "main" "trade" "labour"
#' }
#' @export
db_list_schemas <- function() {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  sql <- glue::glue("
    SELECT DISTINCT schema_name
    FROM information_schema.schemata
    WHERE catalog_name = {.db_sql_quote(catalog)}
    ORDER BY schema_name
  ")

  DBI::dbGetQuery(con, sql)$schema_name
}


#' List tables in a DuckLake schema
#'
#' @description Returns all tables in a given schema.
#' @param schema Schema name (default "main")
#' @return Character vector of table names
#' @examples
#' \dontrun{
#' db_connect()
#' db_tables()
#' # [1] "imports" "exports" "products"
#'
#' db_tables("trade")
#' # [1] "monthly_summary" "annual_totals"
#' }
#' @export
db_tables <- function(schema = "main") {
  schema <- .db_validate_name(schema, "schema")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  sql <- glue::glue("
    SELECT table_name
    FROM information_schema.tables
    WHERE table_catalog = {.db_sql_quote(catalog)}
      AND table_schema  = {.db_sql_quote(schema)}
      AND table_type    = 'BASE TABLE'
    ORDER BY table_name
  ")

  DBI::dbGetQuery(con, sql)$table_name
}


#' List views in a DuckLake schema
#'
#' @description Returns all views in a given schema.
#' @param schema Schema name (default "main")
#' @return Character vector of view names
#' @export
db_list_views <- function(schema = "main") {
  schema <- .db_validate_name(schema, "schema")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  sql <- glue::glue("
    SELECT table_name
    FROM information_schema.tables
    WHERE table_catalog = {.db_sql_quote(catalog)}
      AND table_schema  = {.db_sql_quote(schema)}
      AND table_type    = 'VIEW'
    ORDER BY table_name
  ")

  DBI::dbGetQuery(con, sql)$table_name
}


#' Check if a DuckLake table exists
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return Logical TRUE if exists, FALSE otherwise
#' @examples
#' \dontrun{
#' db_connect()
#' db_table_exists(table = "imports")
#' # [1] TRUE
#' }
#' @export
db_table_exists <- function(schema = "main", table) {
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

  # Use the internal helper
  .db_table_exists(con, catalog, schema, table)
}


#' Create a new schema in DuckLake
#'
#' @description Creates a new schema in the DuckLake catalog.
#'
#' DuckLake automatically organizes data into `{schema}/{table}/` folders
#' under the catalog's DATA_PATH. This default structure enables folder-based
#' access control - simply set ACLs on the schema folders.
#'
#' @param schema Schema name to create
#' @return Invisibly returns the schema name
#' @examples
#' \dontrun{
#' db_connect(data_path = "/data/lake")
#'
#' db_create_schema("trade")
#' db_create_schema("labour")
#'
#' # Data will be organized as:
#' # /data/lake/trade/imports/ducklake-xxx.parquet
#' # /data/lake/trade/exports/ducklake-xxx.parquet
#' # /data/lake/labour/employment/ducklake-xxx.parquet
#'
#' # Set folder ACLs on /data/lake/trade/ to control access
#' }
#' @export
db_create_schema <- function(schema) {
  schema <- .db_validate_name(schema, "schema")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  sql <- glue::glue("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
  DBI::dbExecute(con, sql)

  message("Schema created: ", catalog, ".", schema)
  invisible(schema)
}


#' Set partitioning for a DuckLake table
#'
#' @description Configures partition keys for a DuckLake table. When partitioning
#'   is set, new data written to the table will be split into separate files
#'   based on the partition key values.
#'
#'   Partitioning enables:
#'   - Efficient query pruning (only read relevant partitions)
#'   - Potential folder-based access control at partition level
#'   - Better data organization for time-series data
#'
#'   Note: Existing data is not reorganized - only new inserts are partitioned.
#'
#' @param schema Schema name
#' @param table Table name
#' @param partition_by Character vector of column names or expressions to partition by.
#'   Use NULL to remove partitioning.
#' @return Invisibly returns TRUE on success
#' @examples
#' \dontrun{
#' db_connect(...)
#'
#' # Partition by year and month columns
#' db_set_partitioning("trade", "imports", c("year", "month"))
#'
#' # Partition using date functions
#' db_set_partitioning("trade", "imports", c("year(date)", "month(date)"))
#'
#' # Remove partitioning (new data won't be partitioned)
#' db_set_partitioning("trade", "imports", NULL)
#' }
#' @export
db_set_partitioning <- function(schema = "main", table, partition_by) {
  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", table)

  # Check table exists
  if (!.db_table_exists(con, catalog, schema, table)) {
    stop("Table '", qname, "' does not exist.", call. = FALSE)
  }

  if (is.null(partition_by) || length(partition_by) == 0) {
    # Remove partitioning
    sql <- glue::glue("ALTER TABLE {qname} RESET PARTITIONED BY")
    DBI::dbExecute(con, sql)
    message("Removed partitioning from ", qname)
  } else {
    # Validate partition_by
    if (!is.character(partition_by)) {
      stop("partition_by must be a character vector of column names or expressions.", call. = FALSE)
    }

    # Build partition clause - allow expressions like year(date)
    partition_clause <- paste(partition_by, collapse = ", ")
    sql <- glue::glue("ALTER TABLE {qname} SET PARTITIONED BY ({partition_clause})")

    tryCatch({
      DBI::dbExecute(con, sql)
      message("Set partitioning on ", qname, ": ", partition_clause)
    }, error = function(e) {
      stop("Failed to set partitioning: ", e$message, call. = FALSE)
    })
  }

  invisible(TRUE)
}


#' Get partitioning configuration for a DuckLake table
#'
#' @description Returns the current partition keys configured for a table,
#'   or NULL if the table is not partitioned.
#'
#' @param schema Schema name
#' @param table Table name
#' @return A character vector of partition key expressions, or NULL if not partitioned
#' @examples
#' \dontrun{
#' db_connect(...)
#' db_set_partitioning("trade", "imports", c("year", "month"))
#' db_get_partitioning("trade", "imports")
#' #> [1] "year" "month"
#' }
#' @export
db_get_partitioning <- function(schema = "main", table) {
  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")

  # Query the DuckLake metadata for partition info
  # Metadata schema is __ducklake_metadata_{catalog}
  metadata_schema <- paste0("__ducklake_metadata_", catalog)

  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
    SELECT DISTINCT c.column_name, pc.transform, pc.partition_key_index
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
    data.frame(column_name = character(0), transform = character(0), partition_key_index = integer(0))
  })

  if (nrow(result) == 0) {
    return(NULL)
  }

  # Build partition key expressions
  # If transform is specified, wrap column in transform function
  partition_keys <- vapply(seq_len(nrow(result)), function(i) {
    col <- result$column_name[i]
    transform <- result$transform[i]
    if (!is.na(transform) && nzchar(transform) && transform != "identity") {
      paste0(transform, "(", col, ")")
    } else {
      col
    }
  }, character(1))

  partition_keys
}


# ==============================================================================
# Views
# ==============================================================================

#' Create a view in DuckLake
#'
#' @description Creates a SQL view stored in the DuckLake catalog. Views support
#'   time travel - attaching at a previous snapshot will reflect the view definition
#'   that existed at that point in time.
#'
#' @param schema Schema name (default "main")
#' @param view View name
#' @param query SQL query defining the view
#' @param replace If TRUE, replace existing view (default FALSE)
#' @return Invisibly returns the qualified view name
#' @examples
#' \dontrun{
#' db_connect(...)
#'
#' # Create a simple view
#' db_create_view(view = "active_users",
#'                query = "SELECT * FROM users WHERE active = true")
#'
#' # Replace existing view
#' db_create_view(view = "active_users",
#'                query = "SELECT * FROM users WHERE active = true AND verified = true",
#'                replace = TRUE)
#'
#' # View with aggregation
#' db_create_view(schema = "reports", view = "monthly_totals",
#'                query = "SELECT year, month, SUM(value) as total FROM sales GROUP BY year, month")
#' }
#' @seealso [db_list_views()], [db_drop_view()]
#' @export
db_create_view <- function(schema = "main", view, query, replace = FALSE) {
  schema <- .db_validate_name(schema, "schema")
  view <- .db_validate_name(view, "view")

  if (missing(query) || !is.character(query) || length(query) != 1 || !nzchar(query)) {
    stop("query must be a non-empty SQL string.", call. = FALSE)
  }

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", view)

  create_keyword <- if (replace) "CREATE OR REPLACE VIEW" else "CREATE VIEW"
  sql <- glue::glue("{create_keyword} {qname} AS {query}")

  tryCatch({
    DBI::dbExecute(con, sql)
    message("Created view: ", qname)
  }, error = function(e) {
    stop("Failed to create view: ", e$message, call. = FALSE)
  })

  invisible(qname)
}


#' Drop a view from DuckLake
#'
#' @param schema Schema name (default "main")
#' @param view View name
#' @param if_exists If TRUE, don't error if view doesn't exist (default FALSE)
#' @return Invisibly returns TRUE on success
#' @export
db_drop_view <- function(schema = "main", view, if_exists = FALSE) {
  schema <- .db_validate_name(schema, "schema")
  view <- .db_validate_name(view, "view")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", view)
  exists_clause <- if (if_exists) "IF EXISTS " else ""
  sql <- glue::glue("DROP VIEW {exists_clause}{qname}")

  DBI::dbExecute(con, sql)
  message("Dropped view: ", qname)

  invisible(TRUE)
}


# ==============================================================================
# Macros
# ==============================================================================

#' Create a macro in DuckLake
#'
#' @description Creates a SQL macro stored in the DuckLake catalog. Macros are
#'   reusable SQL expressions that can be scalar (return a single value) or
#'   table-valued (return a table). Macros support time travel.
#'
#' @param schema Schema name (default "main")
#' @param name Macro name
#' @param params Character vector of parameter names, or named character vector
#'   with types (e.g., `c(a = "INTEGER", b = "VARCHAR")`)
#' @param body SQL expression for the macro body
#' @param table_macro If TRUE, create a table macro (returns a table). Default FALSE.
#' @param replace If TRUE, replace existing macro (default FALSE)
#' @return Invisibly returns the qualified macro name
#' @examples
#' \dontrun{
#' db_connect(...)
#'
#' # Scalar macro
#' db_create_macro(name = "add_values",
#'                 params = c("a", "b"),
#'                 body = "a + b")
#'
#' # Table macro
#' db_create_macro(name = "filtered_sales",
#'                 params = c(min_value = "INTEGER"),
#'                 body = "SELECT * FROM sales WHERE value > min_value",
#'                 table_macro = TRUE)
#'
#' # Use the macros
#' db_query("SELECT add_values(10, 20)")
#' db_query("SELECT * FROM filtered_sales(100)")
#' }
#' @seealso [db_list_macros()], [db_drop_macro()]
#' @export
db_create_macro <- function(schema = "main", name, params = character(0),
                            body, table_macro = FALSE, replace = FALSE) {
  schema <- .db_validate_name(schema, "schema")
  name <- .db_validate_name(name, "macro name")

  if (missing(body) || !is.character(body) || length(body) != 1 || !nzchar(body)) {
    stop("body must be a non-empty SQL string.", call. = FALSE)
  }

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", name)

  # Build parameter list
  if (length(params) == 0) {
    param_clause <- ""
  } else if (is.null(names(params)) || all(names(params) == "")) {
    # Untyped parameters
    param_clause <- paste(params, collapse = ", ")
  } else {
    # Typed parameters
    param_clause <- paste(names(params), params, collapse = ", ")
  }

  create_keyword <- if (replace) "CREATE OR REPLACE MACRO" else "CREATE MACRO"
  table_keyword <- if (table_macro) " AS TABLE " else " AS "

  sql <- glue::glue("{create_keyword} {qname}({param_clause}){table_keyword}{body}")

  tryCatch({
    DBI::dbExecute(con, sql)
    message("Created macro: ", qname)
  }, error = function(e) {
    stop("Failed to create macro: ", e$message, call. = FALSE)
  })

  invisible(qname)
}


#' List macros in a DuckLake schema
#'
#' @param schema Schema name (default "main")
#' @return Character vector of macro names
#' @export
db_list_macros <- function(schema = "main") {
  schema <- .db_validate_name(schema, "schema")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  # Query DuckLake metadata for macros
  metadata_schema <- paste0("__ducklake_metadata_", catalog)

  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT m.macro_name
      FROM {metadata_schema}.ducklake_macro m
      JOIN {metadata_schema}.ducklake_schema s ON m.schema_id = s.schema_id
      WHERE s.schema_name = '{schema}'
      ORDER BY m.macro_name
    "))
  }, error = function(e) {
    # Fallback to duckdb_functions if metadata query fails
    tryCatch({
      DBI::dbGetQuery(con, glue::glue("
        SELECT DISTINCT function_name as macro_name
        FROM duckdb_functions()
        WHERE schema_name = '{catalog}.{schema}'
          AND function_type = 'macro'
        ORDER BY function_name
      "))
    }, error = function(e2) {
      data.frame(macro_name = character(0))
    })
  })

  result$macro_name
}


#' Drop a macro from DuckLake
#'
#' @param schema Schema name (default "main")
#' @param name Macro name
#' @param if_exists If TRUE, don't error if macro doesn't exist (default FALSE)
#' @return Invisibly returns TRUE on success
#' @export
db_drop_macro <- function(schema = "main", name, if_exists = FALSE) {
  schema <- .db_validate_name(schema, "schema")
  name <- .db_validate_name(name, "macro name")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", name)
  exists_clause <- if (if_exists) "IF EXISTS " else ""
  sql <- glue::glue("DROP MACRO {exists_clause}{qname}")

  DBI::dbExecute(con, sql)
  message("Dropped macro: ", qname)

  invisible(TRUE)
}


# ==============================================================================
# Comments (SQL COMMENT ON)
# ==============================================================================

#' Add comment/metadata to table or column
#'
#' @description Adds metadata to a table or column using DuckLake's native
#'   COMMENT ON statement. Comments are stored in the DuckLake catalog and
#'   support time travel.
#'
#'   The comment can be a simple string or a list with structured metadata.
#'   Lists are automatically converted to JSON for storage.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param column Optional column name. If NULL, comment is added to the table.
#' @param comment The comment - either a string or a list. Lists are converted
#'   to JSON. Use NULL to remove comment.
#'
#'   For tables, common list fields: `description`, `owner`, `tags`, `lineage_sources`,
#'   `lineage_transformation`.
#'
#'   For columns, common list fields: `description`, `units`, `notes`.
#'
#' @return Invisibly returns TRUE on success
#' @examples
#' \dontrun{
#' db_connect(...)
#'
#' # Simple string comment
#' db_comment(table = "users", comment = "Active user accounts")
#'
#' # Structured table metadata (stored as JSON)
#' db_comment(table = "imports", comment = list(
#'   description = "Monthly import values by country",
#'   owner = "Trade Section",
#'   tags = c("trade", "monthly", "official")
#' ))
#'
#' # Structured column metadata
#' db_comment(table = "imports", column = "value", comment = list(
#'   description = "Import value",
#'   units = "EUR (thousands)"
#' ))
#'
#' # Remove comment
#' db_comment(table = "users", comment = NULL)
#' }
#' @seealso [db_get_docs()] to retrieve documentation
#' @export
db_comment <- function(schema = "main", table, column = NULL, comment) {
  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  qname <- paste0(catalog, ".", schema, ".", table)

  # Convert list to JSON string
  if (is.list(comment)) {
    # Remove NULL values from the list
    comment <- comment[!sapply(comment, is.null)]
    if (length(comment) == 0) {
      comment <- NULL
    } else {
      comment <- jsonlite::toJSON(comment, auto_unbox = TRUE)
    }
  }

  if (is.null(column)) {
    # Table comment
    if (is.null(comment)) {
      sql <- glue::glue("COMMENT ON TABLE {qname} IS NULL")
    } else {
      sql <- glue::glue("COMMENT ON TABLE {qname} IS {.db_sql_quote(as.character(comment))}")
    }
    DBI::dbExecute(con, sql)
    message("Set comment on table ", qname)
  } else {
    # Column comment
    column <- .db_validate_name(column, "column")
    if (is.null(comment)) {
      sql <- glue::glue("COMMENT ON COLUMN {qname}.{column} IS NULL")
    } else {
      sql <- glue::glue("COMMENT ON COLUMN {qname}.{column} IS {.db_sql_quote(as.character(comment))}")
    }
    DBI::dbExecute(con, sql)
    message("Set comment on column ", qname, ".", column)
  }

  invisible(TRUE)
}


# ==============================================================================
# Logging
# ==============================================================================

#' Enable DuckDB/DuckLake logging
#'
#' @description Enables logging for debugging and monitoring DuckLake operations.
#'   DuckLake registers a dedicated log type for metadata queries. The built-in
#'   QueryLog type can trace all SQL queries including internal ones.
#'
#' @param enable If TRUE, enable logging. If FALSE, disable.
#' @param log_type Type of logging: "query" for SQL queries, "metadata" for
#'   DuckLake metadata operations, or "all" for both. Default "query".
#' @return Invisibly returns TRUE on success
#' @examples
#' \dontrun{
#' db_connect(...)
#'
#' # Enable query logging
#' db_enable_logging(TRUE)
#'
#' # Run some operations...
#' db_read(table = "users") |> head()
#'
#' # View logs
#' db_query("SELECT * FROM duckdb_logs() ORDER BY timestamp DESC LIMIT 20")
#'
#' # Disable logging
#' db_enable_logging(FALSE)
#' }
#' @export
db_enable_logging <- function(enable = TRUE, log_type = c("query", "metadata", "all")) {
  log_type <- match.arg(log_type)

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  if (enable) {
    # Enable logging based on type
    if (log_type %in% c("query", "all")) {
      DBI::dbExecute(con, "SET enable_logging = true")
    }
    message("Logging enabled (", log_type, "). View with: db_query(\"SELECT * FROM duckdb_logs()\")")
  } else {
    DBI::dbExecute(con, "SET enable_logging = false")
    message("Logging disabled")
  }

  invisible(TRUE)
}
