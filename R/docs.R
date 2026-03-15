# R/docs.R
# Documentation and metadata functions for DuckLake tables
# Uses DuckLake's native ducklake_metadata table for storage

# ==============================================================================
# Internal Helpers for DuckLake Metadata
# ==============================================================================

#' Get the DuckLake metadata schema name
#' @noRd
.db_metadata_schema <- function(catalog) {
 paste0("__ducklake_metadata_", catalog)
}

#' Get table_id from DuckLake metadata
#' @noRd
.db_get_table_id <- function(con, catalog, schema, table) {
  metadata_schema <- .db_metadata_schema(catalog)

  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT t.table_id
      FROM {metadata_schema}.ducklake_table t
      JOIN {metadata_schema}.ducklake_schema s ON t.schema_id = s.schema_id
      WHERE s.schema_name = {.db_sql_quote(schema)}
        AND t.table_name = {.db_sql_quote(table)}
        AND t.end_snapshot IS NULL
    "))
  }, error = function(e) data.frame())

  if (nrow(result) == 0) return(NULL)
  result$table_id[1]
}

#' Get column_id from DuckLake metadata
#' @noRd
.db_get_column_id <- function(con, catalog, schema, table, column) {
  metadata_schema <- .db_metadata_schema(catalog)

  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT c.column_id
      FROM {metadata_schema}.ducklake_column c
      JOIN {metadata_schema}.ducklake_table t ON c.table_id = t.table_id
      JOIN {metadata_schema}.ducklake_schema s ON t.schema_id = s.schema_id
      WHERE s.schema_name = {.db_sql_quote(schema)}
        AND t.table_name = {.db_sql_quote(table)}
        AND c.column_name = {.db_sql_quote(column)}
        AND t.end_snapshot IS NULL
    "))
  }, error = function(e) data.frame())

  if (nrow(result) == 0) return(NULL)
  result$column_id[1]
}

#' Set metadata in ducklake_metadata table
#' @noRd
.db_set_metadata <- function(con, catalog, key, value, scope, scope_id) {
  metadata_schema <- .db_metadata_schema(catalog)

  # Delete existing entry
  DBI::dbExecute(con, glue::glue("
    DELETE FROM {metadata_schema}.ducklake_metadata
    WHERE key = {.db_sql_quote(key)}
      AND scope = {.db_sql_quote(scope)}
      AND scope_id = {scope_id}
  "))

  # Insert new value (only if not NULL/empty)
  if (!is.null(value) && nzchar(value)) {
    DBI::dbExecute(con, glue::glue("
      INSERT INTO {metadata_schema}.ducklake_metadata (key, value, scope, scope_id)
      VALUES ({.db_sql_quote(key)}, {.db_sql_quote(value)}, {.db_sql_quote(scope)}, {scope_id})
    "))
  }
}

#' Get metadata from ducklake_metadata table
#' @noRd
.db_get_metadata <- function(con, catalog, key, scope, scope_id) {
  metadata_schema <- .db_metadata_schema(catalog)

  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT value
      FROM {metadata_schema}.ducklake_metadata
      WHERE key = {.db_sql_quote(key)}
        AND scope = {.db_sql_quote(scope)}
        AND scope_id = {scope_id}
    "))
  }, error = function(e) data.frame())

  if (nrow(result) == 0) return(NULL)
  result$value[1]
}


# ==============================================================================
# Core Documentation Functions
# ==============================================================================

#' Describe a table
#'
#' @description Add documentation metadata to a DuckLake table.
#' Metadata includes description, owner, and tags.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param description Free-text description of the table
#' @param owner Owner name or team responsible for this data
#' @param tags Character vector of tags for categorization
#' @return Invisibly returns the metadata list
#'
#' @examples
#' \dontrun{
#' db_connect()
#' db_describe(
#'   table = "imports",
#'   description = "Monthly import values by country and commodity code",
#'   owner = "Trade Section",
#'   tags = c("trade", "monthly", "official")
#' )
#' }
#' @export
db_describe <- function(schema = "main", table,
                        description = NULL, owner = NULL, tags = NULL) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  if (missing(table)) {
    stop("table is required", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")
  catalog <- .db_get("catalog")

  qname <- paste(catalog, schema, table, sep = ".")

  # Get table_id from DuckLake metadata
  table_id <- .db_get_table_id(con, catalog, schema, table)
  if (is.null(table_id)) {
    stop("Table '", qname, "' not found in DuckLake catalog.", call. = FALSE)
  }

  # Store metadata in DuckLake's ducklake_metadata table
  # Using scope='table' and scope_id=table_id
  if (!is.null(description)) {
    .db_set_metadata(con, catalog, "description", description, "table", table_id)
  }
  if (!is.null(owner)) {
    .db_set_metadata(con, catalog, "owner", owner, "table", table_id)
  }
  if (!is.null(tags)) {
    tags_str <- paste(tags, collapse = ",")
    .db_set_metadata(con, catalog, "tags", tags_str, "table", table_id)
  }

  message("Updated metadata for ", qname)

  invisible(list(
    schema = schema, table = table,
    description = description, owner = owner, tags = tags
  ))
}


#' Describe a column
#'
#' @description Add documentation to a specific column in a table.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param column Column name to document
#' @param description Description of what the column contains
#' @param units Units of measurement (optional)
#' @param notes Additional notes (optional)
#' @return Invisibly returns the column metadata
#'
#' @examples
#' \dontrun{
#' db_connect()
#' db_describe_column(
#'   table = "imports",
#'   column = "value",
#'   description = "Import value in thousands of EUR",
#'   units = "EUR thousands"
#' )
#' }
#' @export
db_describe_column <- function(schema = "main", table, column,
                               description = NULL, units = NULL, notes = NULL) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")
  column <- .db_validate_name(column, "column", strict = FALSE)
  catalog <- .db_get("catalog")

  qname <- paste(catalog, schema, table, column, sep = ".")

  # Get column_id from DuckLake metadata
  column_id <- .db_get_column_id(con, catalog, schema, table, column)
  if (is.null(column_id)) {
    stop("Column '", qname, "' not found in DuckLake catalog.", call. = FALSE)
  }

  # Store metadata in DuckLake's ducklake_metadata table
  # Using scope='column' and scope_id=column_id
  if (!is.null(description)) {
    .db_set_metadata(con, catalog, "description", description, "column", column_id)
  }
  if (!is.null(units)) {
    .db_set_metadata(con, catalog, "units", units, "column", column_id)
  }
  if (!is.null(notes)) {
    .db_set_metadata(con, catalog, "notes", notes, "column", column_id)
  }

  message("Updated column metadata for ", catalog, ".", schema, ".", table, ".", column)
  invisible(list(column = column, description = description, units = units, notes = notes))
}


#' Get documentation for a table
#'
#' @description Retrieve documentation metadata for a table.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return A list containing description, owner, tags, and column documentation
#'
#' @examples
#' \dontrun{
#' db_connect()
#' db_get_docs(table = "imports")
#' }
#' @export
db_get_docs <- function(schema = "main", table) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")
  catalog <- .db_get("catalog")
  metadata_schema <- .db_metadata_schema(catalog)

  # Get table_id
  table_id <- .db_get_table_id(con, catalog, schema, table)

  # Build result with defaults
  result <- list(
    schema = schema,
    table = table,
    description = NULL,
    owner = NULL,
    tags = character(0),
    columns = list()
  )

  # Get table-level metadata from ducklake_metadata
  if (!is.null(table_id)) {
    result$description <- .db_get_metadata(con, catalog, "description", "table", table_id)
    result$owner <- .db_get_metadata(con, catalog, "owner", "table", table_id)

    tags_str <- .db_get_metadata(con, catalog, "tags", "table", table_id)
    if (!is.null(tags_str) && nzchar(tags_str)) {
      result$tags <- strsplit(tags_str, ",")[[1]]
    }

    # Get column-level metadata
    # First get all columns for this table
    cols <- tryCatch({
      DBI::dbGetQuery(con, glue::glue("
        SELECT c.column_id, c.column_name
        FROM {metadata_schema}.ducklake_column c
        WHERE c.table_id = {table_id}
      "))
    }, error = function(e) data.frame())

    if (nrow(cols) > 0) {
      for (i in seq_len(nrow(cols))) {
        col_id <- cols$column_id[i]
        col_name <- cols$column_name[i]

        col_desc <- .db_get_metadata(con, catalog, "description", "column", col_id)
        col_units <- .db_get_metadata(con, catalog, "units", "column", col_id)
        col_notes <- .db_get_metadata(con, catalog, "notes", "column", col_id)

        if (!is.null(col_desc) || !is.null(col_units) || !is.null(col_notes)) {
          result$columns[[col_name]] <- list(
            description = col_desc,
            units = col_units,
            notes = col_notes
          )
        }
      }
    }
  }

  result
}


# ==============================================================================
# Data Dictionary
# ==============================================================================

#' Generate a data dictionary
#'
#' @description Creates a data dictionary summarizing all tables
#' with their documentation, schemas, and column information.
#'
#' @param schema Limit to specific schema (optional)
#' @param include_columns Include column-level details (default TRUE)
#' @return A data.frame with the data dictionary
#'
#' @examples
#' \dontrun{
#' db_connect()
#' dict <- db_dictionary()
#'
#' # Export to Excel
#' writexl::write_xlsx(dict, "data_dictionary.xlsx")
#' }
#' @export
db_dictionary <- function(schema = NULL, include_columns = TRUE) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")

  # Get all tables
  schema_clause <- if (!is.null(schema)) {
    glue::glue("AND table_schema = {.db_sql_quote(schema)}")
  } else {
    "AND table_schema != '_metadata'"
  }

  tables_sql <- glue::glue("
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_catalog = {.db_sql_quote(catalog)}
      AND table_type = 'BASE TABLE'
      {schema_clause}
    ORDER BY table_schema, table_name
  ")

  tables <- DBI::dbGetQuery(con, tables_sql)

  if (nrow(tables) == 0) {
    if (include_columns) {
      return(data.frame(
        schema = character(), table = character(),
        description = character(), owner = character(), tags = character(),
        column_name = character(), column_type = character(),
        column_description = character(), column_units = character()
      ))
    } else {
      return(data.frame(
        schema = character(), table = character(),
        description = character(), owner = character(), tags = character(),
        column_count = integer()
      ))
    }
  }

  rows <- list()

  for (i in seq_len(nrow(tables))) {
    tbl_schema <- tables$table_schema[i]
    tbl_name <- tables$table_name[i]
    qname <- paste0(catalog, ".", tbl_schema, ".", tbl_name)

    # Get docs
    docs <- tryCatch(
      db_get_docs(schema = tbl_schema, table = tbl_name),
      error = function(e) list()
    )

    # Get columns
    cols_sql <- glue::glue("
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_catalog = {.db_sql_quote(catalog)}
        AND table_schema = {.db_sql_quote(tbl_schema)}
        AND table_name = {.db_sql_quote(tbl_name)}
      ORDER BY ordinal_position
    ")
    cols <- DBI::dbGetQuery(con, cols_sql)

    # Extract metadata with null safety
    desc <- docs$description
    if (is.null(desc) || length(desc) == 0) desc <- NA_character_
    own <- docs$owner
    if (is.null(own) || length(own) == 0) own <- NA_character_
    tgs <- docs$tags
    if (is.null(tgs) || length(tgs) == 0) tgs <- ""
    tgs <- paste(tgs, collapse = ", ")

    if (include_columns && nrow(cols) > 0) {
      for (j in seq_len(nrow(cols))) {
        col_name <- cols$column_name[j]
        col_meta <- docs$columns[[col_name]] %||% list()
        col_desc <- col_meta$description
        if (is.null(col_desc) || length(col_desc) == 0) col_desc <- NA_character_
        col_units <- col_meta$units
        if (is.null(col_units) || length(col_units) == 0) col_units <- NA_character_

        rows[[length(rows) + 1]] <- data.frame(
          schema = tbl_schema,
          table = tbl_name,
          description = desc,
          owner = own,
          tags = tgs,
          column_name = col_name,
          column_type = cols$data_type[j],
          column_description = col_desc,
          column_units = col_units,
          stringsAsFactors = FALSE
        )
      }
    } else {
      rows[[length(rows) + 1]] <- data.frame(
        schema = tbl_schema,
        table = tbl_name,
        description = desc,
        owner = own,
        tags = tgs,
        column_count = nrow(cols),
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(rows) == 0) {
    if (include_columns) {
      return(data.frame(
        schema = character(), table = character(),
        description = character(), owner = character(), tags = character(),
        column_name = character(), column_type = character(),
        column_description = character(), column_units = character()
      ))
    } else {
      return(data.frame(
        schema = character(), table = character(),
        description = character(), owner = character(), tags = character(),
        column_count = integer()
      ))
    }
  }

  do.call(rbind, rows)
}


# ==============================================================================
# Lineage Tracking
# ==============================================================================

#' Record data lineage
#'
#' @description Records the source(s) of a table for data lineage tracking.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param sources Character vector of source descriptions
#' @param transformation Description of how data was transformed
#' @return Invisibly returns TRUE
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' db_lineage(
#'   table = "monthly_summary",
#'   sources = c("raw.transactions", "raw.products"),
#'   transformation = "Aggregated by month and product category"
#' )
#' }
#' @export
db_lineage <- function(schema = "main", table, sources, transformation = NULL) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")
  catalog <- .db_get("catalog")

  qname <- paste(catalog, schema, table, sep = ".")

  # Get table_id from DuckLake metadata
  table_id <- .db_get_table_id(con, catalog, schema, table)
  if (is.null(table_id)) {
    stop("Table '", qname, "' not found in DuckLake catalog.", call. = FALSE)
  }

  # Store lineage in DuckLake's ducklake_metadata table
  sources_str <- paste(sources, collapse = "; ")
  .db_set_metadata(con, catalog, "lineage_sources", sources_str, "table", table_id)

  if (!is.null(transformation)) {
    .db_set_metadata(con, catalog, "lineage_transformation", transformation, "table", table_id)
  }

  message("Recorded lineage for ", catalog, ".", schema, ".", table)
  invisible(TRUE)
}


#' Get lineage information
#'
#' @description Retrieves lineage information for a table.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return A list with sources and transformation, or NULL if not recorded
#' @export
db_get_lineage <- function(schema = "main", table) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")
  catalog <- .db_get("catalog")

  # Get table_id
  table_id <- .db_get_table_id(con, catalog, schema, table)
  if (is.null(table_id)) return(NULL)

  # Get lineage from ducklake_metadata
  sources_str <- .db_get_metadata(con, catalog, "lineage_sources", "table", table_id)
  if (is.null(sources_str)) return(NULL)

  transformation <- .db_get_metadata(con, catalog, "lineage_transformation", "table", table_id)

  list(
    sources = strsplit(sources_str, "; ")[[1]],
    transformation = transformation
  )
}


# ==============================================================================
# Search Functions
# ==============================================================================

#' Search for tables
#'
#' @description Search for tables by name, description, owner, or tags.
#'
#' @param pattern Search pattern (case-insensitive, matches partial strings)
#' @param field Field to search: "all" (default), "name", "description", "owner", "tags"
#' @return A data.frame of matching tables with their documentation
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Search everywhere
#' db_search("trade")
#'
#' # Search only tags
#' db_search("official", field = "tags")
#'
#' # Search by owner
#' db_search("Trade Section", field = "owner")
#' }
#' @export
db_search <- function(pattern, field = c("all", "name", "description", "owner", "tags")) {

  field <- match.arg(field)

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")

  # Get all tables with their docs
  dict <- db_dictionary(include_columns = FALSE)

  if (nrow(dict) == 0) {
    return(data.frame(
      schema = character(), table = character(),
      description = character(), owner = character(), tags = character()
    ))
  }

  # Search based on field
  pattern_lower <- tolower(pattern)

  matches <- switch(field,
    all = grepl(pattern_lower, tolower(dict$table), fixed = TRUE) |
          grepl(pattern_lower, tolower(dict$description %||% ""), fixed = TRUE) |
          grepl(pattern_lower, tolower(dict$owner %||% ""), fixed = TRUE) |
          grepl(pattern_lower, tolower(dict$tags %||% ""), fixed = TRUE),
    name = grepl(pattern_lower, tolower(dict$table), fixed = TRUE),
    description = grepl(pattern_lower, tolower(dict$description %||% ""), fixed = TRUE),
    owner = grepl(pattern_lower, tolower(dict$owner %||% ""), fixed = TRUE),
    tags = grepl(pattern_lower, tolower(dict$tags %||% ""), fixed = TRUE)
  )

  dict[matches, c("schema", "table", "description", "owner", "tags")]
}


#' Search for columns by name
#'
#' @description Find tables that contain columns matching a pattern.
#'
#' @param pattern Column name pattern (case-insensitive)
#' @param schema Optional schema to limit search
#' @return A data.frame with schema, table, column_name, and column_type
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Find all columns with "country" in the name
#' db_search_columns("country")
#'
#' # Find ID columns
#' db_search_columns("_id")
#' }
#' @export
db_search_columns <- function(pattern, schema = NULL) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")

  schema_clause <- if (!is.null(schema)) {
    glue::glue("AND table_schema = {.db_sql_quote(schema)}")
  } else {
    "AND table_schema != '_metadata'"
  }

  sql <- glue::glue("
    SELECT table_schema AS schema, table_name AS table,
           column_name, data_type AS column_type
    FROM information_schema.columns
    WHERE table_catalog = {.db_sql_quote(catalog)}
      {schema_clause}
      AND LOWER(column_name) LIKE LOWER({.db_sql_quote(paste0('%', pattern, '%'))})
    ORDER BY table_schema, table_name, ordinal_position
  ")

  DBI::dbGetQuery(con, sql)
}
