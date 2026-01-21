# R/discovery.R

# ---- Hive Discovery ----

#' List sections in the hive data lake
#'
#' @description Returns all top-level section folders in the data lake.
#' @return Character vector of section names
#' @examples
#' \dontrun{
#' db_connect()
#' db_list_sections()
#' # [1] "Trade" "Labour" "Health" "Agriculture"
#' }
#' @export
db_list_sections <- function() {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "hive") {
    stop("Connected in DuckLake mode. Use db_list_schemas() instead, or reconnect with db_connect().", call. = FALSE)
  }

  base_path <- .db_get("data_path")
  if (is.null(base_path)) {
    stop("No data path configured.", call. = FALSE)
  }

  if (!dir.exists(base_path)) {
    stop("Data path does not exist or is not accessible: ", base_path, call. = FALSE)
  }

  # List directories only (sections are folders)
  all_items <- list.dirs(base_path, full.names = FALSE, recursive = FALSE)

 # Filter out hidden folders and system folders (like _catalog)
  all_items[!grepl("^[\\._]", all_items)]
}


#' List datasets within a section
#'
#' @description Returns all datasets (subfolders) within a given section.
#' @param section The section name (e.g. "Trade")
#' @return Character vector of dataset names
#' @examples
#' \dontrun{
#' db_connect()
#' db_list_datasets("Trade")
#' # [1] "Imports" "Exports" "Balance"
#' }
#' @export
db_list_datasets <- function(section) {
  section <- .db_validate_name(section, "section")

 con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "hive") {
    stop("Connected in DuckLake mode. Use db_list_tables() instead, or reconnect with db_connect().", call. = FALSE)
  }

  base_path <- .db_get("data_path")
  if (is.null(base_path)) {
    stop("No data path configured.", call. = FALSE)
  }

  section_path <- file.path(base_path, section)

  if (!dir.exists(section_path)) {
    stop("Section does not exist or is not accessible: ", section, call. = FALSE)
  }

  # List directories only (datasets are folders)
  all_items <- list.dirs(section_path, full.names = FALSE, recursive = FALSE)

  # Filter out hidden folders and partition folders (year=, month=, etc.)
  all_items <- all_items[!grepl("^\\.", all_items)]
  all_items[!grepl("=", all_items)]
}


#' Check if a hive dataset exists
#'
#' @param section The section name
#' @param dataset The dataset name
#' @return Logical TRUE if exists, FALSE otherwise
#' @examples
#' \dontrun{
#' db_connect()
#' db_dataset_exists("Trade", "Imports")
#' # [1] TRUE
#' }
#' @export
db_dataset_exists <- function(section, dataset) {
  section <- .db_validate_name(section, "section")
  dataset <- .db_validate_name(dataset, "dataset")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "hive") {
    stop("Connected in DuckLake mode. Use db_table_exists() instead, or reconnect with db_connect().", call. = FALSE)
  }

  base_path <- .db_get("data_path")
  if (is.null(base_path)) {
    stop("No data path configured.", call. = FALSE)
  }

  dataset_path <- file.path(base_path, section, dataset)

  # Check folder exists and contains at least one parquet file
 if (!dir.exists(dataset_path)) {
    return(FALSE)
  }

  # Look for any parquet files (including in partition subfolders)
  parquet_files <- list.files(dataset_path, pattern = "\\.parquet$", recursive = TRUE)
  length(parquet_files) > 0
}


# ---- DuckLake Discovery ----

#' List schemas in the DuckLake catalog
#'
#' @description Returns all schemas in the connected DuckLake catalog.
#' @return Character vector of schema names
#' @examples
#' \dontrun{
#' db_lake_connect()
#' db_list_schemas()
#' # [1] "main" "trade" "labour"
#' }
#' @export
db_list_schemas <- function() {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Use db_list_sections() instead, or reconnect with db_lake_connect().", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
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
#' db_lake_connect()
#' db_list_tables()
#' # [1] "imports" "exports" "products"
#'
#' db_list_tables("trade")
#' # [1] "monthly_summary" "annual_totals"
#' }
#' @export
db_list_tables <- function(schema = "main") {
  schema <- .db_validate_name(schema, "schema")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Use db_list_datasets() instead, or reconnect with db_lake_connect().", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
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
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Views are only available for DuckLake.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
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
#' db_lake_connect()
#' db_table_exists(table = "imports")
#' # [1] TRUE
#' }
#' @export
db_table_exists <- function(schema = "main", table) {
  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Use db_dataset_exists() instead, or reconnect with db_lake_connect().", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
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
#' db_lake_connect(data_path = "//CSO-NAS/DataLake")
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
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Schemas are only available for DuckLake.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
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
#' db_lake_connect(...)
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
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Partitioning is only available for DuckLake.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_lake_connect() first.", call. = FALSE)
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
#' db_lake_connect(...)
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
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Partitioning is only available for DuckLake.", call. = FALSE)
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
      JOIN {metadata_schema}.ducklake_column c ON pc.column_id = c.column_id
      WHERE s.schema_name = '{schema}' AND t.table_name = '{table}'
      ORDER BY pc.partition_key_index
    "))
  }, error = function(e) {
    # Table may not exist or metadata table may differ
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
