# R/docs.R
# Documentation and metadata functions for DuckLake tables

# ==============================================================================
# Metadata Table Management
# ==============================================================================

#' Ensure metadata schema and tables exist
#' @description Creates metadata tables in the base DuckDB connection (not in DuckLake catalog)
#' since DuckLake doesn't support regular table creation.
#' @noRd
.db_ensure_metadata_table <- function(con, catalog) {
  # Create metadata schema in the main DuckDB connection (not DuckLake catalog)
  # We use a fixed schema name that won't conflict with user schemas
  metadata_schema <- "_datapond_metadata"

 tryCatch({
    DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS {metadata_schema}"))
  }, error = function(e) {
    # Schema might already exist
  })

  # Create table_docs table
  table_docs_exists <- tryCatch({
    DBI::dbExecute(con, glue::glue("
      CREATE TABLE IF NOT EXISTS {metadata_schema}.table_docs (
        catalog_name VARCHAR NOT NULL,
        schema_name VARCHAR NOT NULL,
        table_name VARCHAR NOT NULL,
        description VARCHAR,
        owner VARCHAR,
        tags VARCHAR,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (catalog_name, schema_name, table_name)
      )
    "))
    TRUE
  }, error = function(e) {
    # Check if table already exists
    tryCatch({
      DBI::dbGetQuery(con, glue::glue("SELECT 1 FROM {metadata_schema}.table_docs LIMIT 0"))
      TRUE
    }, error = function(e2) FALSE)
  })

  # Create column_docs table
  column_docs_exists <- tryCatch({
    DBI::dbExecute(con, glue::glue("
      CREATE TABLE IF NOT EXISTS {metadata_schema}.column_docs (
        catalog_name VARCHAR NOT NULL,
        schema_name VARCHAR NOT NULL,
        table_name VARCHAR NOT NULL,
        column_name VARCHAR NOT NULL,
        description VARCHAR,
        units VARCHAR,
        notes VARCHAR,
        updated_at TIMESTAMP,
        PRIMARY KEY (catalog_name, schema_name, table_name, column_name)
      )
    "))
    TRUE
  }, error = function(e) {
    tryCatch({
      DBI::dbGetQuery(con, glue::glue("SELECT 1 FROM {metadata_schema}.column_docs LIMIT 0"))
      TRUE
    }, error = function(e2) FALSE)
  })

  if (!table_docs_exists || !column_docs_exists) {
    stop(
      "Failed to create metadata tables.\n",
      "This may indicate a DuckDB configuration issue.",
      call. = FALSE
    )
  }

  invisible(TRUE)
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

  # Create metadata schema and table if needed
  .db_ensure_metadata_table(con, catalog)

  qname <- paste(catalog, schema, table, sep = ".")

  # Check if record exists and preserve created_at if so
  existing <- DBI::dbGetQuery(con, glue::glue("
    SELECT created_at, description, owner, tags
    FROM _datapond_metadata.table_docs
    WHERE catalog_name = {.db_sql_quote(catalog)}
      AND schema_name = {.db_sql_quote(schema)}
      AND table_name = {.db_sql_quote(table)}
  "))

  if (nrow(existing) > 0) {
    # Update existing record, preserving created_at and merging fields
    new_desc <- if (!is.null(description)) .db_sql_quote(description) else if (!is.na(existing$description[1])) .db_sql_quote(existing$description[1]) else "NULL"
    new_owner <- if (!is.null(owner)) .db_sql_quote(owner) else if (!is.na(existing$owner[1])) .db_sql_quote(existing$owner[1]) else "NULL"
    new_tags <- if (!is.null(tags)) .db_sql_quote(paste(tags, collapse = ",")) else if (!is.na(existing$tags[1])) .db_sql_quote(existing$tags[1]) else "NULL"

    DBI::dbExecute(con, glue::glue("
      UPDATE _datapond_metadata.table_docs
      SET description = {new_desc},
          owner = {new_owner},
          tags = {new_tags},
          updated_at = NOW()
      WHERE catalog_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
    "))
  } else {
    # Insert new record
    meta_sql <- glue::glue("
      INSERT INTO _datapond_metadata.table_docs
      (catalog_name, schema_name, table_name, description, owner, tags, created_at, updated_at)
      VALUES (
        {.db_sql_quote(catalog)},
        {.db_sql_quote(schema)},
        {.db_sql_quote(table)},
        {if (is.null(description)) 'NULL' else .db_sql_quote(description)},
        {if (is.null(owner)) 'NULL' else .db_sql_quote(owner)},
        {if (is.null(tags)) 'NULL' else .db_sql_quote(paste(tags, collapse = ','))},
        NOW(),
        NOW()
      )
    ")
    DBI::dbExecute(con, meta_sql)
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

  .db_ensure_metadata_table(con, catalog)

  # Use upsert-style logic
  existing <- DBI::dbGetQuery(con, glue::glue("
    SELECT description, units, notes
    FROM _datapond_metadata.column_docs
    WHERE catalog_name = {.db_sql_quote(catalog)}
      AND schema_name = {.db_sql_quote(schema)}
      AND table_name = {.db_sql_quote(table)}
      AND column_name = {.db_sql_quote(column)}
  "))

  if (nrow(existing) > 0) {
    # Update - merge with existing
    new_desc <- if (!is.null(description)) .db_sql_quote(description) else if (!is.na(existing$description[1])) .db_sql_quote(existing$description[1]) else "NULL"
    new_units <- if (!is.null(units)) .db_sql_quote(units) else if (!is.na(existing$units[1])) .db_sql_quote(existing$units[1]) else "NULL"
    new_notes <- if (!is.null(notes)) .db_sql_quote(notes) else if (!is.na(existing$notes[1])) .db_sql_quote(existing$notes[1]) else "NULL"

    DBI::dbExecute(con, glue::glue("
      UPDATE _datapond_metadata.column_docs
      SET description = {new_desc},
          units = {new_units},
          notes = {new_notes},
          updated_at = NOW()
      WHERE catalog_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
        AND column_name = {.db_sql_quote(column)}
    "))
  } else {
    # Insert
    meta_sql <- glue::glue("
      INSERT INTO _datapond_metadata.column_docs
      (catalog_name, schema_name, table_name, column_name, description, units, notes, updated_at)
      VALUES (
        {.db_sql_quote(catalog)},
        {.db_sql_quote(schema)},
        {.db_sql_quote(table)},
        {.db_sql_quote(column)},
        {if (is.null(description)) 'NULL' else .db_sql_quote(description)},
        {if (is.null(units)) 'NULL' else .db_sql_quote(units)},
        {if (is.null(notes)) 'NULL' else .db_sql_quote(notes)},
        NOW()
      )
    ")
    DBI::dbExecute(con, meta_sql)
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

  # Check if metadata tables exist
  meta_exists <- tryCatch({
    DBI::dbGetQuery(con, "SELECT 1 FROM _datapond_metadata.table_docs LIMIT 0")
    TRUE
  }, error = function(e) FALSE)

  if (!meta_exists) {
    return(list(
      schema = schema, table = table,
      description = NULL, owner = NULL, tags = character(0),
      columns = list()
    ))
  }

  # Get table docs
  table_doc <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT description, owner, tags
      FROM _datapond_metadata.table_docs
      WHERE catalog_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
    "))
  }, error = function(e) data.frame())

  # Get column docs
  col_docs <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT column_name, description, units, notes
      FROM _datapond_metadata.column_docs
      WHERE catalog_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
    "))
  }, error = function(e) data.frame())

  # Build result
  result <- list(
    schema = schema,
    table = table,
    description = if (nrow(table_doc) > 0) table_doc$description[1] else NULL,
    owner = if (nrow(table_doc) > 0) table_doc$owner[1] else NULL,
    tags = if (nrow(table_doc) > 0 && !is.na(table_doc$tags[1])) {
      strsplit(table_doc$tags[1], ",")[[1]]
    } else character(0),
    columns = list()
  )

  if (nrow(col_docs) > 0) {
    for (i in seq_len(nrow(col_docs))) {
      result$columns[[col_docs$column_name[i]]] <- list(
        description = col_docs$description[i],
        units = col_docs$units[i],
        notes = col_docs$notes[i]
      )
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

  # Ensure lineage table exists in base DuckDB (not DuckLake catalog)
  tryCatch({
    DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS _datapond_metadata")
    DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS _datapond_metadata.lineage (
        catalog_name VARCHAR NOT NULL,
        schema_name VARCHAR NOT NULL,
        table_name VARCHAR NOT NULL,
        sources VARCHAR,
        transformation VARCHAR,
        recorded_at TIMESTAMP,
        PRIMARY KEY (catalog_name, schema_name, table_name)
      )
    ")
  }, error = function(e) NULL)

  sources_str <- paste(sources, collapse = "; ")

  # Upsert
  DBI::dbExecute(con, glue::glue("
    DELETE FROM _datapond_metadata.lineage
    WHERE catalog_name = {.db_sql_quote(catalog)}
      AND schema_name = {.db_sql_quote(schema)}
      AND table_name = {.db_sql_quote(table)}
  "))

  DBI::dbExecute(con, glue::glue("
    INSERT INTO _datapond_metadata.lineage
    (catalog_name, schema_name, table_name, sources, transformation, recorded_at)
    VALUES (
      {.db_sql_quote(catalog)},
      {.db_sql_quote(schema)},
      {.db_sql_quote(table)},
      {.db_sql_quote(sources_str)},
      {if (is.null(transformation)) 'NULL' else .db_sql_quote(transformation)},
      NOW()
    )
  "))

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

  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT sources, transformation, recorded_at
      FROM _datapond_metadata.lineage
      WHERE catalog_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
    "))
  }, error = function(e) data.frame())

  if (nrow(result) == 0) return(NULL)

  list(
    sources = strsplit(result$sources[1], "; ")[[1]],
    transformation = result$transformation[1],
    recorded_at = result$recorded_at[1]
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
