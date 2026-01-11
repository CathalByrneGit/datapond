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
#' @description Creates a new schema in the DuckLake catalog. In DuckLake 0.2+,
#'   schemas can have custom data paths, enabling folder-based access control.
#' @param schema Schema name to create
#' @param path Optional data path for this schema. Files for tables in this
#'   schema will be stored under this path. Use this to enable folder-based
#'   access control (e.g., different teams have access to different paths).
#' @return Invisibly returns the schema name
#' @examples
#' \dontrun{
#' db_lake_connect()
#'
#' # Simple schema (uses default data path)
#' db_create_schema("reference")
#'
#' # Schema with custom path for access control
#' db_create_schema("trade", path = "//CSO-NAS/DataLake/trade/")
#' db_create_schema("labour", path = "//CSO-NAS/DataLake/labour/")
#'
#' # Now folder ACLs on //CSO-NAS/DataLake/trade/ control access to trade schema
#' }
#' @export
db_create_schema <- function(schema, path = NULL) {
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

  # Create the schema
  sql <- glue::glue("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
  DBI::dbExecute(con, sql)

  # If path is provided, update the ducklake metadata to set the custom path
  # DuckLake 0.2+ stores path info in __ducklake_metadata_{catalog}.ducklake_schema
  if (!is.null(path)) {
    # Normalize path and determine if it's absolute
    path <- normalizePath(path, mustWork = FALSE)
    is_absolute <- grepl("^(/|[A-Za-z]:)", path)

    tryCatch({
      # Update the schema's path in the metadata table
      # Metadata schema is __ducklake_metadata_{catalog}
      metadata_schema <- paste0("__ducklake_metadata_", catalog)
      update_sql <- glue::glue("
        UPDATE {metadata_schema}.ducklake_schema
        SET path = '{path}',
            path_is_relative = {if (is_absolute) 'false' else 'true'}
        WHERE schema_name = '{schema}'
      ")
      DBI::dbExecute(con, update_sql)
    }, error = function(e) {
      warning("Could not set schema path (DuckLake 0.2+ required): ", e$message, call. = FALSE)
    })

    message("Schema created: ", catalog, ".", schema, " (path: ", path, ")")
  } else {
    message("Schema created: ", catalog, ".", schema)
  }
  invisible(schema)
}


#' Get the data path for a schema
#'
#' @description Returns the data path configured for a DuckLake schema.
#'   Returns NULL if the schema uses the default catalog data path.
#' @param schema Schema name
#' @return The path string, or NULL if using default
#' @examples
#' \dontrun{
#' db_lake_connect()
#' db_create_schema("trade", path = "//CSO-NAS/trade/")
#' db_get_schema_path("trade")
#' #> "//CSO-NAS/trade/"
#' }
#' @export
db_get_schema_path <- function(schema) {
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

  # Query the DuckLake metadata for schema path
  # Metadata schema is __ducklake_metadata_{catalog}
  metadata_schema <- paste0("__ducklake_metadata_", catalog)
  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT path FROM {metadata_schema}.ducklake_schema
      WHERE schema_name = '{schema}'
    "))
  }, error = function(e) {
    # Schema may not exist or metadata table may differ
    data.frame(path = character(0))
  })

  if (nrow(result) == 0 || is.na(result$path[1]) || result$path[1] == "") {
    return(NULL)
  }

  path <- result$path[1]

  # DuckLake sets default path to {schema_name}/ - treat as NULL (no custom path)
  default_path <- paste0(schema, "/")
  if (path == default_path) {
    return(NULL)
  }

  path
}


#' Get the data path for a table
#'
#' @description Returns the data path configured for a DuckLake table.
#'   Returns NULL if the table uses the default path (relative to schema).
#' @param schema Schema name
#' @param table Table name
#' @return The path string, or NULL if using default
#' @examples
#' \dontrun{
#' db_lake_connect()
#' db_get_table_path("trade", "imports")
#' }
#' @export
db_get_table_path <- function(schema, table) {

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")
  if (!is.null(curr_mode) && curr_mode != "ducklake") {
    stop("Connected in hive mode. Table paths are only available for DuckLake.", call. = FALSE)
  }

  catalog <- .db_get("catalog")

  # Query the DuckLake metadata for table path

  # Metadata schema is __ducklake_metadata_{catalog}
  metadata_schema <- paste0("__ducklake_metadata_", catalog)
  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT t.path
      FROM {metadata_schema}.ducklake_table t
      JOIN {metadata_schema}.ducklake_schema s ON t.schema_id = s.schema_id
      WHERE s.schema_name = '{schema}' AND t.table_name = '{table}'
    "))
  }, error = function(e) {
    # Table may not exist or metadata table may differ
    data.frame(path = character(0))
  })

  if (nrow(result) == 0 || is.na(result$path[1]) || result$path[1] == "") {
    return(NULL)
  }

  path <- result$path[1]

  # DuckLake sets default path to {table_name}/ - treat as NULL (no custom path)
  default_path <- paste0(table, "/")
  if (path == default_path) {
    return(NULL)
  }

  path
}
