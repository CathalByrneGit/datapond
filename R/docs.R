# R/docs.R
# Documentation and metadata functions for DuckLake tables
# Uses DuckLake's native COMMENT ON for storage (survives disconnect/reconnect)

# ==============================================================================
# Internal Helpers
# ==============================================================================
#' Parse JSON comment or return as-is if not JSON
#' @noRd
.db_parse_comment <- function(comment_str) {
  if (is.null(comment_str) || !nzchar(comment_str)) {
    return(NULL)
  }

  # Try to parse as JSON
  tryCatch({
    parsed <- jsonlite::fromJSON(comment_str)
    if (is.list(parsed)) {
      return(parsed)
    }
    # If it parsed but isn't a list (e.g., just a string in quotes), return as-is
    return(list(description = comment_str))
  }, error = function(e) {
    # Not JSON, treat as plain description
    return(list(description = comment_str))
  })
}

#' Get table comment from DuckDB catalog
#' @noRd
.db_get_table_comment <- function(con, catalog, schema, table) {
  result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT comment
      FROM duckdb_tables()
      WHERE database_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
    "))
  }, error = function(e) data.frame())

  if (nrow(result) == 0 || is.na(result$comment[1])) {
    return(NULL)
  }
  result$comment[1]
}

#' Get column comment from DuckDB catalog
#' @noRd
.db_get_column_comment <- function(con, catalog, schema, table, column) {
result <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT comment
      FROM duckdb_columns()
      WHERE database_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
        AND column_name = {.db_sql_quote(column)}
    "))
  }, error = function(e) data.frame())

  if (nrow(result) == 0 || is.na(result$comment[1])) {
    return(NULL)
  }
  result$comment[1]
}


# ==============================================================================
# Documentation Functions
# ==============================================================================

#' Get documentation for a table
#'
#' @description Retrieve documentation metadata for a table and its columns.
#'   Metadata is stored using native SQL COMMENT ON statements.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return A list containing description, owner, tags, lineage, and column documentation
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # First, add documentation
#' db_comment(table = "imports", comment = list(
#'   description = "Monthly import values",
#'   owner = "Trade Section",
#'   tags = c("trade", "monthly")
#' ))
#'
#' # Then retrieve it
#' db_get_docs(table = "imports")
#' }
#' @seealso [db_comment()] to add documentation
#' @export
db_get_docs <- function(schema = "main", table) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table <- .db_validate_name(table, "table")
  catalog <- .db_get("catalog")

  # Build result with defaults
  result <- list(
    schema = schema,
    table = table,
    description = NULL,
    owner = NULL,
    tags = character(0),
    lineage = NULL,
    columns = list()
  )

  # Get table comment and parse
  table_comment <- .db_get_table_comment(con, catalog, schema, table)
  if (!is.null(table_comment)) {
    parsed <- .db_parse_comment(table_comment)
    if (!is.null(parsed)) {
      result$description <- parsed$description
      result$owner <- parsed$owner
      if (!is.null(parsed$tags)) {
        result$tags <- if (is.character(parsed$tags)) parsed$tags else as.character(parsed$tags)
      }
      # Lineage info
      if (!is.null(parsed$lineage_sources) || !is.null(parsed$lineage_transformation)) {
        result$lineage <- list(
          sources = parsed$lineage_sources,
          transformation = parsed$lineage_transformation
        )
      }
    }
  }

  # Get column comments
  cols <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT column_name, comment
      FROM duckdb_columns()
      WHERE database_name = {.db_sql_quote(catalog)}
        AND schema_name = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
    "))
  }, error = function(e) data.frame())

  if (nrow(cols) > 0) {
    for (i in seq_len(nrow(cols))) {
      col_name <- cols$column_name[i]
      col_comment <- cols$comment[i]

      if (!is.na(col_comment) && nzchar(col_comment)) {
        parsed <- .db_parse_comment(col_comment)
        if (!is.null(parsed)) {
          result$columns[[col_name]] <- parsed
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
#'   Lineage is stored in the table's comment as JSON metadata alongside
#'   description, owner, and tags.
#'
#'   When `pipeline` is supplied (a lazy dbplyr table), column-level lineage
#'   is extracted automatically using the `dplyneage` package (if installed).
#'   This traces exactly which source columns produce which output columns,
#'   through joins, aggregations, and computed expressions.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param sources Character vector of source table names. Inferred from
#'   `pipeline` if not supplied.
#' @param transformation Description of how data was transformed
#' @param pipeline Optional lazy dbplyr table (the pipeline that produced the
#'   table). If supplied and `dplyneage` is installed, column-level lineage is
#'   extracted automatically. `sources` can be omitted when `pipeline` is given.
#' @return Invisibly returns TRUE
#'
#' @examples
#' \dontrun{
#' db_connect()
#'
#' # Simple manual lineage
#' db_lineage(
#'   table = "monthly_summary",
#'   sources = c("raw.transactions", "raw.products"),
#'   transformation = "Aggregated by month and product category"
#' )
#'
#' # Automatic column-level lineage from a dbplyr pipeline (requires dplyneage)
#' pipeline <- db_read(table = "imports") |>
#'   dplyr::group_by(country) |>
#'   dplyr::summarise(total = sum(value))
#'
#' db_lineage(table = "country_totals", pipeline = pipeline)
#' }
#' @seealso [db_get_lineage()] to retrieve lineage, [db_lineage_flow()] to visualise
#' @export
db_lineage <- function(schema = "main", table, sources = NULL,
                       transformation = NULL, pipeline = NULL) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")
  catalog <- .db_get("catalog")
  qname  <- paste(catalog, schema, table, sep = ".")

  if (!.db_table_exists(con, catalog, schema, table)) {
    stop("Table '", qname, "' not found.", call. = FALSE)
  }

  # Extract column-level lineage from pipeline via dplyneage
  col_edges <- NULL
  if (!is.null(pipeline)) {
    if (!inherits(pipeline, c("tbl_lazy", "tbl_sql"))) {
      stop("'pipeline' must be a lazy dbplyr table (tbl_lazy/tbl_sql).", call. = FALSE)
    }
    if (!requireNamespace("dplyneage", quietly = TRUE)) {
      message("Install dplyneage for automatic column-level lineage: ",
              "pak::pak('tgerke/dplyneage')")
    } else {
      lin <- tryCatch(
        dplyneage::extract_lineage(pipeline),
        error = function(e) {
          message("Could not extract column lineage: ", e$message)
          NULL
        }
      )
      if (!is.null(lin)) {
        edges <- dplyneage::lineage_edges(lin)
        col_edges <- as.list(edges)

        # Infer sources from pipeline if not provided
        if (is.null(sources)) {
          sources <- unique(edges$source_table)
          sources <- sources[nzchar(sources)]
        }
      }
    }
  }

  if (is.null(sources) && is.null(col_edges)) {
    stop("Provide 'sources' or 'pipeline'.", call. = FALSE)
  }

  # Merge into existing comment metadata
  existing_comment <- .db_get_table_comment(con, catalog, schema, table)
  existing_meta <- if (!is.null(existing_comment)) {
    .db_parse_comment(existing_comment)
  } else {
    list()
  }

  if (!is.null(sources))       existing_meta$lineage_sources        <- sources
  if (!is.null(transformation)) existing_meta$lineage_transformation <- transformation
  if (!is.null(col_edges))     existing_meta$lineage_column_edges   <- col_edges

  db_comment(schema = schema, table = table, comment = existing_meta)

  level <- if (!is.null(col_edges)) "column-level" else "table-level"
  message("Recorded ", level, " lineage for ", qname)
  invisible(TRUE)
}


#' Get lineage information
#'
#' @description Retrieves lineage information stored for a table.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return A list with `sources`, `transformation`, and (if available)
#'   `column_edges` data frame with columns: source_table, source_column,
#'   target_table, target_column, transformation, expression.
#'   Returns NULL if no lineage has been recorded.
#' @seealso [db_lineage()] to record lineage, [db_lineage_flow()] to visualise
#' @export
db_get_lineage <- function(schema = "main", table) {

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")
  catalog <- .db_get("catalog")

  table_comment <- .db_get_table_comment(con, catalog, schema, table)
  if (is.null(table_comment)) return(NULL)

  parsed <- .db_parse_comment(table_comment)
  if (is.null(parsed$lineage_sources) && is.null(parsed$lineage_column_edges)) {
    return(NULL)
  }

  result <- list(
    sources        = parsed$lineage_sources,
    transformation = parsed$lineage_transformation
  )

  if (!is.null(parsed$lineage_column_edges)) {
    result$column_edges <- as.data.frame(parsed$lineage_column_edges)
  }

  result
}


#' Visualise column-level lineage as an interactive diagram
#'
#' @description Renders an interactive React Flow lineage diagram for a table,
#'   showing how source columns flow into output columns. Requires `dplyneage`.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @return An htmlwidget (displayed in the viewer or browser)
#' @examples
#' \dontrun{
#' db_connect()
#' db_lineage_flow(table = "country_totals")
#' }
#' @seealso [db_lineage()] to record lineage, [db_get_lineage()] to retrieve it
#' @export
db_lineage_flow <- function(schema = "main", table) {
  if (!requireNamespace("dplyneage", quietly = TRUE)) {
    stop("Install dplyneage to visualise lineage: pak::pak('tgerke/dplyneage')",
         call. = FALSE)
  }

  lin <- db_get_lineage(schema = schema, table = table)
  if (is.null(lin)) {
    stop("No lineage recorded for this table. Run db_lineage() first.", call. = FALSE)
  }
  if (is.null(lin$column_edges)) {
    stop("Only table-level lineage is available. Re-run db_lineage(pipeline = ...) ",
         "for column-level lineage.", call. = FALSE)
  }

  # Reconstruct a lineage object from stored edges and render
  dplyneage::lineage_flow(
    dplyneage::lineage_from_edges(lin$column_edges)
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
