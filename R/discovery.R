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
#' db_connect(data_path = "//CSO-NAS/DataLake")
#'
#' db_create_schema("trade")
#' db_create_schema("labour")
#'
#' # Data will be organized as:
#' # //CSO-NAS/DataLake/trade/imports/ducklake-xxx.parquet
#' # //CSO-NAS/DataLake/trade/exports/ducklake-xxx.parquet
#' # //CSO-NAS/DataLake/labour/employment/ducklake-xxx.parquet
#'
#' # Set folder ACLs on //CSO-NAS/DataLake/trade/ to control access
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
