# R/docs.R
# Documentation and metadata functions for datasets and tables

# ==============================================================================
# Metadata Storage Helpers
# ==============================================================================

#' Get path to metadata file for a hive dataset
#' @noRd
.db_metadata_path <- function(section, dataset) {

  base_path <- .db_get("data_path")
  file.path(base_path, section, dataset, "_metadata.json")
}

# ==============================================================================
# Public Catalog Helpers (Hive Mode)
# ==============================================================================

#' Get path to the public catalog folder
#' @noRd
.db_catalog_path <- function() {
  base_path <- .db_get("data_path")
  file.path(base_path, "_catalog")
}

#' Get path to public metadata file for a dataset
#' @noRd
.db_public_metadata_path <- function(section, dataset) {
  catalog_path <- .db_catalog_path()
  file.path(catalog_path, section, paste0(dataset, ".json"))
}

#' Copy metadata to public catalog
#' @noRd
.db_publish_metadata <- function(section, dataset) {
  # Source: dataset's _metadata.json
  source_path <- .db_metadata_path(section, dataset)
  if (!file.exists(source_path)) {
    stop("No metadata exists for ", section, "/", dataset, ". Use db_describe() first.",
         call. = FALSE)
  }

  # Destination: _catalog/section/dataset.json
  dest_path <- .db_public_metadata_path(section, dataset)
  dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)

  # Copy with additional catalog metadata
  metadata <- .db_read_metadata(source_path)
  metadata$catalog_published_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  metadata$section <- section
  metadata$dataset <- dataset
  metadata$public <- TRUE

  jsonlite::write_json(metadata, dest_path, pretty = TRUE, auto_unbox = TRUE)
  invisible(dest_path)
}

#' Remove metadata from public catalog
#' @noRd
.db_unpublish_metadata <- function(section, dataset) {
  dest_path <- .db_public_metadata_path(section, dataset)
  if (file.exists(dest_path)) {
    file.remove(dest_path)
  }
  invisible(TRUE)
}

#' Sync public catalog entry if dataset is public
#' @noRd
.db_sync_public_catalog <- function(section, dataset) {
  # Check if already public
  public_path <- .db_public_metadata_path(section, dataset)
  if (file.exists(public_path)) {
    # Re-publish to sync
    .db_publish_metadata(section, dataset)
    return(TRUE)
  }
  invisible(FALSE)
}

#' Read metadata from JSON file
#' @noRd
.db_read_metadata <- function(path) {

  if (!file.exists(path)) {
    return(list(
      description = NULL,
      owner = NULL,
      tags = character(0),
      columns = list(),
      created_at = NULL,
      updated_at = NULL
    ))
  }
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

#' Write metadata to JSON file
#' @noRd
.db_write_metadata <- function(metadata, path) {
  # Ensure directory exists

dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  # Update timestamp
  metadata$updated_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  if (is.null(metadata$created_at)) {
    metadata$created_at <- metadata$updated_at
  }
  
  jsonlite::write_json(metadata, path, pretty = TRUE, auto_unbox = TRUE)
  invisible(metadata)
}

# ==============================================================================
# Core Documentation Functions
# ==============================================================================

#' Describe a dataset or table
#'
#' @description Add documentation metadata to a dataset (hive mode) or table
#' (DuckLake mode). Metadata includes description, owner, and tags.
#'
#' In hive mode, you can set `public = TRUE` to publish the metadata to a
#' shared catalog folder, making it discoverable organisation-wide without
#' granting access to the underlying data.
#'
#' @param section Section name (hive mode only)
#' @param dataset Dataset name (hive mode only)
#' @param schema Schema name (DuckLake mode, default "main")
#' @param table Table name (DuckLake mode only)
#' @param description Free-text description of the dataset/table
#' @param owner Owner name or team responsible for this data
#' @param tags Character vector of tags for categorization
#' @param public Logical. If TRUE, publish metadata to the shared catalog folder.
#'   If FALSE, remove from catalog. If NULL (default), keep current public status
#'   and auto-sync if already public. (Hive mode only)
#' @return Invisibly returns the metadata list
#'
#' @examples
#' \dontrun{
#' # Hive mode
#' db_connect()
#' db_describe(
#'   section = "Trade",
#'   dataset = "Imports",
#'   description = "Monthly import values by country and commodity code",
#'   owner = "Trade Section",
#'   tags = c("trade", "monthly", "official"),
#'   public = TRUE
#' )
#'
#' # DuckLake mode
#' db_lake_connect()
#' db_describe(
#'   table = "imports",
#'   description = "Monthly import values",
#'   owner = "Trade Section"
#' )
#' }
#' @export
db_describe <- function(section = NULL, dataset = NULL,
                        schema = "main", table = NULL,
                        description = NULL, owner = NULL, tags = NULL,
                        public = NULL) {


  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")

  if (curr_mode == "hive") {
    # Hive mode - store in JSON sidecar file
    if (is.null(section) || is.null(dataset)) {
      stop("section and dataset are required in hive mode.", call. = FALSE)
    }

    section <- .db_validate_name(section, "section")
    dataset <- .db_validate_name(dataset, "dataset")

    meta_path <- .db_metadata_path(section, dataset)
    metadata <- .db_read_metadata(meta_path)

    # Update fields (only if provided)
    if (!is.null(description)) metadata$description <- description
    if (!is.null(owner)) metadata$owner <- owner
    if (!is.null(tags)) metadata$tags <- as.character(tags)

    .db_write_metadata(metadata, meta_path)

    # Handle public catalog
    if (isTRUE(public)) {
      .db_publish_metadata(section, dataset)
      message("Updated metadata for ", section, "/", dataset, " (published to catalog)")
    } else if (isFALSE(public)) {
      .db_unpublish_metadata(section, dataset)
      message("Updated metadata for ", section, "/", dataset, " (removed from catalog)")
    } else {
      # NULL: auto-sync if already public
      synced <- .db_sync_public_catalog(section, dataset)
      if (synced) {
        message("Updated metadata for ", section, "/", dataset, " (synced to catalog)")
      } else {
        message("Updated metadata for ", section, "/", dataset)
      }
    }

    invisible(metadata)

  } else {
    # DuckLake mode - store in metadata table
    if (is.null(table)) {
      stop("table is required in DuckLake mode.", call. = FALSE)
    }

    schema <- .db_validate_name(schema, "schema")
    table <- .db_validate_name(table, "table")
    catalog <- .db_get("catalog")

    # Create metadata schema and table if needed
    .db_ensure_metadata_table(con, catalog)

    qname <- paste(catalog, schema, table, sep = ".")

    # Delete existing and insert new (DuckDB doesn't support INSERT OR REPLACE)
    DBI::dbExecute(con, glue::glue("
      DELETE FROM {catalog}._metadata.table_docs
      WHERE schema_name = {.db_sql_quote(schema)} AND table_name = {.db_sql_quote(table)}
    "))

    meta_sql <- glue::glue("
      INSERT INTO {catalog}._metadata.table_docs
      (schema_name, table_name, description, owner, tags, created_at, updated_at)
      VALUES (
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

    # Handle public catalog (master discovery catalog)
    section <- .db_get("section")
    if (!is.null(section)) {
      if (isTRUE(public)) {
        .db_publish_to_master(schema, table)
        message("Updated metadata for ", qname, " (published to master catalog)")
      } else if (isFALSE(public)) {
        .db_unpublish_from_master(schema, table)
        message("Updated metadata for ", qname, " (removed from master catalog)")
      } else {
        # NULL: auto-sync if already public
        if (.db_is_public_in_master(schema, table)) {
          .db_publish_to_master(schema, table)
          message("Updated metadata for ", qname, " (synced to master catalog)")
        } else {
          message("Updated metadata for ", qname)
        }
      }
    } else {
      message("Updated metadata for ", qname)
      if (!is.null(public)) {
        message("Note: public parameter ignored - no section set. ",
                "Use db_lake_connect_section() to enable master catalog sync.")
      }
    }

    invisible(list(
      schema = schema, table = table,
      description = description, owner = owner, tags = tags
    ))
  }
}


#' Describe a column
#'
#' @description Add documentation to a specific column in a dataset or table.
#'
#' In hive mode, you can set `public = TRUE` to include the column documentation
#' in the public catalog. The dataset must already be public (use
#' `db_describe(public = TRUE)` first).
#'
#' @param section Section name (hive mode only)
#' @param dataset Dataset name (hive mode only)
#' @param schema Schema name (DuckLake mode, default "main")
#' @param table Table name (DuckLake mode only)
#' @param column Column name to document
#' @param description Description of what the column contains
#' @param units Units of measurement (optional)
#' @param notes Additional notes (optional)
#' @param public Logical. If TRUE, sync column docs to public catalog (requires
#'   dataset to already be public). If NULL (default), auto-sync if dataset is
#'   already public. (Hive mode only)
#' @return Invisibly returns the column metadata
#'
#' @examples
#' \dontrun{
#' db_connect()
#' db_describe_column(
#'   section = "Trade",
#'   dataset = "Imports",
#'   column = "value",
#'   description = "Import value in thousands",
#'   units = "EUR (thousands)",
#'   public = TRUE
#' )
#' }
#' @export
db_describe_column <- function(section = NULL, dataset = NULL,
                                schema = "main", table = NULL,
                                column, description = NULL,
                                units = NULL, notes = NULL,
                                public = NULL) {

  column <- .db_validate_name(column, "column")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")

  if (curr_mode == "hive") {
    if (is.null(section) || is.null(dataset)) {
      stop("section and dataset are required in hive mode.", call. = FALSE)
    }

    section <- .db_validate_name(section, "section")
    dataset <- .db_validate_name(dataset, "dataset")

    meta_path <- .db_metadata_path(section, dataset)
    metadata <- .db_read_metadata(meta_path)

    # Initialize columns list if needed
    if (is.null(metadata$columns)) metadata$columns <- list()

    # Update column metadata
    col_meta <- metadata$columns[[column]] %||% list()
    if (!is.null(description)) col_meta$description <- description
    if (!is.null(units)) col_meta$units <- units
    if (!is.null(notes)) col_meta$notes <- notes

    metadata$columns[[column]] <- col_meta

    .db_write_metadata(metadata, meta_path)

    # Handle public catalog sync
    is_public <- file.exists(.db_public_metadata_path(section, dataset))

    if (isTRUE(public)) {
      if (!is_public) {
        stop("Dataset ", section, "/", dataset, " is not public. ",
             "Use db_describe(public = TRUE) first.", call. = FALSE)
      }
      .db_publish_metadata(section, dataset)
      message("Updated column metadata for ", section, "/", dataset, ".", column,
              " (synced to catalog)")
    } else if (is.null(public) && is_public) {
      # Auto-sync if dataset is already public
      .db_publish_metadata(section, dataset)
      message("Updated column metadata for ", section, "/", dataset, ".", column,
              " (synced to catalog)")
    } else {
      message("Updated column metadata for ", section, "/", dataset, ".", column)
    }

    invisible(col_meta)

  } else {
    if (is.null(table)) {
      stop("table is required in DuckLake mode.", call. = FALSE)
    }
    
    schema <- .db_validate_name(schema, "schema")
    table <- .db_validate_name(table, "table")
    catalog <- .db_get("catalog")
    
    .db_ensure_metadata_table(con, catalog)
    
    # Delete existing and insert new
    DBI::dbExecute(con, glue::glue("
      DELETE FROM {catalog}._metadata.column_docs 
      WHERE schema_name = {.db_sql_quote(schema)} 
        AND table_name = {.db_sql_quote(table)}
        AND column_name = {.db_sql_quote(column)}
    "))
    
    meta_sql <- glue::glue("
      INSERT INTO {catalog}._metadata.column_docs 
      (schema_name, table_name, column_name, description, units, notes, updated_at)
      VALUES (
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
    
    message("Updated column metadata for ", catalog, ".", schema, ".", table, ".", column)
    invisible(list(column = column, description = description, units = units, notes = notes))
  }
}


#' Get documentation for a dataset or table
#' 
#' @description Retrieve documentation metadata for a dataset or table.
#' 
#' @param section Section name (hive mode only)
#' @param dataset Dataset name (hive mode only)
#' @param schema Schema name (DuckLake mode, default "main")
#' @param table Table name (DuckLake mode only)
#' @return A list containing description, owner, tags, and column documentation
#' 
#' @examples
#' \dontrun{
#' db_connect()
#' db_get_docs("Trade", "Imports")
#' }
#' @export
db_get_docs <- function(section = NULL, dataset = NULL,
                        schema = "main", table = NULL) {
  
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }
  
  curr_mode <- .db_get("mode")
  
  if (curr_mode == "hive") {
    if (is.null(section) || is.null(dataset)) {
      stop("section and dataset are required in hive mode.", call. = FALSE)
    }
    
    section <- .db_validate_name(section, "section")
    dataset <- .db_validate_name(dataset, "dataset")
    
    meta_path <- .db_metadata_path(section, dataset)
    .db_read_metadata(meta_path)
    
  } else {
    if (is.null(table)) {
      stop("table is required in DuckLake mode.", call. = FALSE)
    }
    
    schema <- .db_validate_name(schema, "schema")
    table <- .db_validate_name(table, "table")
    catalog <- .db_get("catalog")
    
    # Check if metadata tables exist
    meta_exists <- tryCatch({
      DBI::dbGetQuery(con, glue::glue(
        "SELECT 1 FROM information_schema.tables 
         WHERE table_catalog = {.db_sql_quote(catalog)}
         AND table_schema = '_metadata' LIMIT 1"
      ))
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
        FROM {catalog}._metadata.table_docs
        WHERE schema_name = {.db_sql_quote(schema)} 
          AND table_name = {.db_sql_quote(table)}
      "))
    }, error = function(e) data.frame())
    
    # Get column docs
    col_docs <- tryCatch({
      DBI::dbGetQuery(con, glue::glue("
        SELECT column_name, description, units, notes
        FROM {catalog}._metadata.column_docs
        WHERE schema_name = {.db_sql_quote(schema)} 
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
}


# ==============================================================================
# Data Dictionary
# ==============================================================================

#' Generate a data dictionary
#' 
#' @description Creates a data dictionary summarizing all datasets/tables
#' with their documentation, schemas, and column information.
#' 
#' @param section Limit to specific section (hive mode, optional)
#' @param schema Limit to specific schema (DuckLake mode, optional)
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
db_dictionary <- function(section = NULL, schema = NULL, include_columns = TRUE) {
  
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }
  
  curr_mode <- .db_get("mode")
  
  if (curr_mode == "hive") {
    .db_dictionary_hive(con, section, include_columns)
  } else {
    .db_dictionary_ducklake(con, schema, include_columns)
  }
}


#' Generate hive mode data dictionary
#' @noRd
.db_dictionary_hive <- function(con, section_filter, include_columns) {
  base_path <- .db_get("data_path")
  
  # Get sections
 sections <- if (!is.null(section_filter)) {
    section_filter
  } else {
    db_list_sections()
  }
  
  rows <- list()
  
  for (sec in sections) {
    datasets <- db_list_datasets(sec)
    
    for (ds in datasets) {
      # Get metadata
      meta <- tryCatch(
        db_get_docs(section = sec, dataset = ds),
        error = function(e) list()
      )
      
      # Get actual columns from parquet files
      dataset_path <- file.path(base_path, sec, ds)
      cols_info <- tryCatch({
        # Normalize path to use forward slashes (DuckDB requirement)
        norm_path <- gsub("\\\\", "/", dataset_path)
        # Use glob array to match both root-level and subdirectory parquet files
        glob_expr <- glue::glue("['{norm_path}/*.parquet', '{norm_path}/**/*.parquet']")
        sql <- glue::glue("DESCRIBE SELECT * FROM read_parquet({glob_expr}, union_by_name=true) LIMIT 0")
        result <- DBI::dbGetQuery(con, sql)
        # Normalize column names (DuckDB DESCRIBE may return 'column_name'/'column_type' or other variants)
        if (nrow(result) > 0) {
          names(result) <- tolower(names(result))
          # Handle common variations
          if ("name" %in% names(result) && !"column_name" %in% names(result)) {
            names(result)[names(result) == "name"] <- "column_name"
          }
          if ("type" %in% names(result) && !"column_type" %in% names(result)) {
            names(result)[names(result) == "type"] <- "column_type"
          }
          # Ensure required columns exist
          if (!"column_name" %in% names(result)) {
            result$column_name <- NA_character_
          }
          if (!"column_type" %in% names(result)) {
            result$column_type <- NA_character_
          }
        }
        result
      }, error = function(e) NULL)
      
      # Skip if we couldn't read schema
      if (is.null(cols_info) || nrow(cols_info) == 0) {
        # Extract with explicit length safety
        desc <- meta$description
        if (is.null(desc) || length(desc) == 0) desc <- NA_character_
        own <- meta$owner
        if (is.null(own) || length(own) == 0) own <- NA_character_
        tgs <- meta$tags
        if (is.null(tgs) || length(tgs) == 0) tgs <- ""
        tgs <- paste(tgs, collapse = ", ")
        
        # Still add dataset row without column details
        row_data <- data.frame(
          section = sec,
          dataset = ds,
          description = desc,
          owner = own,
          tags = tgs,
          stringsAsFactors = FALSE
        )
        if (include_columns) {
          row_data$column_name <- NA_character_
          row_data$column_type <- NA_character_
          row_data$column_description <- NA_character_
          row_data$column_units <- NA_character_
        } else {
          row_data$column_count <- NA_integer_
        }
        rows[[length(rows) + 1]] <- row_data
        next
      }
      
      if (include_columns) {
        # One row per column
        for (i in seq_len(nrow(cols_info))) {
          col_name <- cols_info$column_name[i]
          if (is.null(col_name) || length(col_name) == 0) col_name <- NA_character_
          col_type <- if ("column_type" %in% names(cols_info)) cols_info$column_type[i] else NA_character_
          if (is.null(col_type) || length(col_type) == 0) col_type <- NA_character_
          col_meta <- if (!is.na(col_name)) (meta$columns[[col_name]] %||% list()) else list()
          
          # Extract with explicit length safety
          desc <- meta$description
          if (is.null(desc) || length(desc) == 0) desc <- NA_character_
          own <- meta$owner
          if (is.null(own) || length(own) == 0) own <- NA_character_
          tgs <- meta$tags
          if (is.null(tgs) || length(tgs) == 0) tgs <- ""
          tgs <- paste(tgs, collapse = ", ")
          col_desc <- col_meta$description
          if (is.null(col_desc) || length(col_desc) == 0) col_desc <- NA_character_
          col_units <- col_meta$units
          if (is.null(col_units) || length(col_units) == 0) col_units <- NA_character_
          
          rows[[length(rows) + 1]] <- data.frame(
            section = sec,
            dataset = ds,
            description = desc,
            owner = own,
            tags = tgs,
            column_name = col_name,
            column_type = col_type,
            column_description = col_desc,
            column_units = col_units,
            stringsAsFactors = FALSE
          )
        }
      } else {
        # One row per dataset - with explicit length safety
        desc <- meta$description
        if (is.null(desc) || length(desc) == 0) desc <- NA_character_
        own <- meta$owner
        if (is.null(own) || length(own) == 0) own <- NA_character_
        tgs <- meta$tags
        if (is.null(tgs) || length(tgs) == 0) tgs <- ""
        tgs <- paste(tgs, collapse = ", ")
        col_cnt <- if (!is.null(cols_info) && nrow(cols_info) > 0) nrow(cols_info) else NA_integer_
        
        rows[[length(rows) + 1]] <- data.frame(
          section = sec,
          dataset = ds,
          description = desc,
          owner = own,
          tags = tgs,
          column_count = col_cnt,
          stringsAsFactors = FALSE
        )
      }
    }
  }
  
  if (length(rows) == 0) {
    if (include_columns) {
      return(data.frame(
        section = character(), dataset = character(),
        description = character(), owner = character(), tags = character(),
        column_name = character(), column_type = character(),
        column_description = character(), column_units = character()
      ))
    } else {
      return(data.frame(
        section = character(), dataset = character(),
        description = character(), owner = character(), tags = character(),
        column_count = integer()
      ))
    }
  }
  
  do.call(rbind, rows)
}


#' Generate DuckLake mode data dictionary
#' @noRd
.db_dictionary_ducklake <- function(con, schema_filter, include_columns) {
  catalog <- .db_get("catalog")
  
  # Get all tables
  schema_clause <- if (!is.null(schema_filter)) {
    glue::glue("AND table_schema = {.db_sql_quote(schema_filter)}")
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
    sch <- tables$table_schema[i]
    tbl <- tables$table_name[i]
    
    # Get docs
    meta <- tryCatch(
      db_get_docs(schema = sch, table = tbl),
      error = function(e) list()
    )
    
    # Get columns
    cols_sql <- glue::glue("
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_catalog = {.db_sql_quote(catalog)}
        AND table_schema = {.db_sql_quote(sch)}
        AND table_name = {.db_sql_quote(tbl)}
      ORDER BY ordinal_position
    ")
    cols_info <- DBI::dbGetQuery(con, cols_sql)
    
    if (include_columns && nrow(cols_info) > 0) {
      for (j in seq_len(nrow(cols_info))) {
        col_name <- cols_info$column_name[j]
        col_meta <- meta$columns[[col_name]] %||% list()
        
        rows[[length(rows) + 1]] <- data.frame(
          schema = sch,
          table = tbl,
          description = meta$description %||% NA_character_,
          owner = meta$owner %||% NA_character_,
          tags = paste(meta$tags, collapse = ", "),
          column_name = col_name,
          column_type = cols_info$data_type[j],
          column_description = col_meta$description %||% NA_character_,
          column_units = col_meta$units %||% NA_character_,
          stringsAsFactors = FALSE
        )
      }
    } else {
      rows[[length(rows) + 1]] <- data.frame(
        schema = sch,
        table = tbl,
        description = meta$description %||% NA_character_,
        owner = meta$owner %||% NA_character_,
        tags = paste(meta$tags, collapse = ", "),
        column_count = nrow(cols_info),
        stringsAsFactors = FALSE
      )
    }
  }
  
  do.call(rbind, rows)
}


# ==============================================================================
# Search
# ==============================================================================

#' Search for datasets or tables
#' 
#' @description Search for datasets/tables by name, description, owner, or tags.
#' 
#' @param pattern Search pattern (case-insensitive, matches partial strings)
#' @param field Field to search: "all" (default), "name", "description", "owner", "tags"
#' @return A data.frame of matching datasets/tables with their documentation
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
  pattern <- tolower(pattern)
  
  # Get full dictionary (without columns for speed)
  dict <- db_dictionary(include_columns = FALSE)
  
  if (nrow(dict) == 0) {
    return(dict)
  }
  
  curr_mode <- .db_get("mode")
  
  # Build search condition
  if (curr_mode == "hive") {
    name_col <- "dataset"
  } else {
    name_col <- "table"
  }
  
  matches <- switch(field,
    all = {
      grepl(pattern, tolower(dict[[name_col]]), fixed = TRUE) |
      grepl(pattern, tolower(dict$description), fixed = TRUE) |
      grepl(pattern, tolower(dict$owner), fixed = TRUE) |
      grepl(pattern, tolower(dict$tags), fixed = TRUE)
    },
    name = grepl(pattern, tolower(dict[[name_col]]), fixed = TRUE),
    description = grepl(pattern, tolower(dict$description), fixed = TRUE),
    owner = grepl(pattern, tolower(dict$owner), fixed = TRUE),
    tags = grepl(pattern, tolower(dict$tags), fixed = TRUE)
  )
  
  # Handle NAs
  matches[is.na(matches)] <- FALSE
  
  dict[matches, , drop = FALSE]
}


#' Search for columns
#' 
#' @description Search for columns by name across all datasets/tables.
#' 
#' @param pattern Column name pattern (case-insensitive, matches partial strings)
#' @return A data.frame of matching columns with their table/dataset info
#' 
#' @examples
#' \dontrun{
#' db_connect()
#' 
#' # Find all columns containing "country"
#' db_search_columns("country")
#' 
#' # Find all ID columns
#' db_search_columns("_id")
#' }
#' @export
db_search_columns <- function(pattern) {
  
  pattern <- tolower(pattern)
  
  # Get full dictionary with columns
  dict <- db_dictionary(include_columns = TRUE)
  
  if (nrow(dict) == 0 || !"column_name" %in% names(dict)) {
    return(dict)
  }
  
  matches <- grepl(pattern, tolower(dict$column_name), fixed = TRUE)
  matches[is.na(matches)] <- FALSE
  
  dict[matches, , drop = FALSE]
}


# ==============================================================================
# Public Catalog Management (Hive Mode)
# ==============================================================================

#' Make a dataset/table discoverable in the public catalog
#'
#' @description Makes metadata discoverable organisation-wide.
#'
#' In hive mode: Copies metadata to the shared `_catalog/` folder.
#' In DuckLake mode: Publishes to the master discovery catalog (requires section).
#'
#' @param section Section name (hive mode), or NULL in DuckLake mode
#' @param dataset Dataset name (hive mode only)
#' @param schema Schema name (DuckLake mode, default "main")
#' @param table Table name (DuckLake mode only)
#' @return Invisibly returns TRUE
#'
#' @examples
#' \dontrun{
#' # Hive mode
#' db_connect("//CSO-NAS/DataLake")
#' db_set_public(section = "Trade", dataset = "Imports")
#'
#' # DuckLake mode
#' db_lake_connect_section("trade")
#' db_set_public(schema = "main", table = "imports")
#' }
#' @export
db_set_public <- function(section = NULL, dataset = NULL,
                          schema = "main", table = NULL) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")

  if (curr_mode == "hive") {
    if (is.null(section) || is.null(dataset)) {
      stop("section and dataset are required in hive mode.", call. = FALSE)
    }
    section <- .db_validate_name(section, "section")
    dataset <- .db_validate_name(dataset, "dataset")

    .db_publish_metadata(section, dataset)
    message("Published ", section, "/", dataset, " to public catalog")
  } else {
    # DuckLake mode
    if (is.null(table)) {
      stop("table is required in DuckLake mode.", call. = FALSE)
    }
    schema <- .db_validate_name(schema, "schema")
    table <- .db_validate_name(table, "table")

    curr_section <- .db_get("section")
    if (is.null(curr_section)) {
      stop("No section set. Use db_lake_connect_section() first.", call. = FALSE)
    }

    .db_publish_to_master(schema, table)
    message("Published ", schema, ".", table, " to master catalog")
  }

  invisible(TRUE)
}


#' Remove a dataset/table from the public catalog
#'
#' @description Removes metadata from the public discovery catalog.
#' The dataset/table and its data remain unchanged.
#'
#' @param section Section name (hive mode), or NULL in DuckLake mode
#' @param dataset Dataset name (hive mode only)
#' @param schema Schema name (DuckLake mode, default "main")
#' @param table Table name (DuckLake mode only)
#' @return Invisibly returns TRUE
#'
#' @examples
#' \dontrun{
#' # Hive mode
#' db_connect("//CSO-NAS/DataLake")
#' db_set_private(section = "Trade", dataset = "Imports")
#'
#' # DuckLake mode
#' db_lake_connect_section("trade")
#' db_set_private(schema = "main", table = "imports")
#' }
#' @export
db_set_private <- function(section = NULL, dataset = NULL,
                            schema = "main", table = NULL) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")

  if (curr_mode == "hive") {
    if (is.null(section) || is.null(dataset)) {
      stop("section and dataset are required in hive mode.", call. = FALSE)
    }
    section <- .db_validate_name(section, "section")
    dataset <- .db_validate_name(dataset, "dataset")

    .db_unpublish_metadata(section, dataset)
    message("Removed ", section, "/", dataset, " from public catalog")
  } else {
    # DuckLake mode
    if (is.null(table)) {
      stop("table is required in DuckLake mode.", call. = FALSE)
    }
    schema <- .db_validate_name(schema, "schema")
    table <- .db_validate_name(table, "table")

    curr_section <- .db_get("section")
    if (is.null(curr_section)) {
      stop("No section set. Use db_lake_connect_section() first.", call. = FALSE)
    }

    .db_unpublish_from_master(schema, table)
    message("Removed ", schema, ".", table, " from master catalog")
  }

  invisible(TRUE)
}


#' Check if a dataset/table is in the public catalog
#'
#' @description Check whether metadata has been published to the discovery catalog.
#'
#' @param section Section name (hive mode), or NULL in DuckLake mode
#' @param dataset Dataset name (hive mode only)
#' @param schema Schema name (DuckLake mode, default "main")
#' @param table Table name (DuckLake mode only)
#' @return Logical TRUE if public, FALSE otherwise
#'
#' @examples
#' \dontrun{
#' # Hive mode
#' db_connect("//CSO-NAS/DataLake")
#' db_is_public(section = "Trade", dataset = "Imports")
#'
#' # DuckLake mode
#' db_lake_connect_section("trade")
#' db_is_public(schema = "main", table = "imports")
#' }
#' @export
db_is_public <- function(section = NULL, dataset = NULL,
                          schema = "main", table = NULL) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")

  if (curr_mode == "hive") {
    if (is.null(section) || is.null(dataset)) {
      stop("section and dataset are required in hive mode.", call. = FALSE)
    }
    section <- .db_validate_name(section, "section")
    dataset <- .db_validate_name(dataset, "dataset")

    path <- .db_public_metadata_path(section, dataset)
    file.exists(path)
  } else {
    # DuckLake mode
    if (is.null(table)) {
      stop("table is required in DuckLake mode.", call. = FALSE)
    }
    schema <- .db_validate_name(schema, "schema")
    table <- .db_validate_name(table, "table")

    .db_is_public_in_master(schema, table)
  }
}


#' List all datasets/tables in the public catalog
#'
#' @description Lists all entries published to the discovery catalog.
#' This works even if you don't have access to the underlying data,
#' allowing organisation-wide data discovery.
#'
#' @param section Optional section to filter by
#' @return A data.frame with discovery information
#'
#' @examples
#' \dontrun{
#' # Hive mode
#' db_connect("//CSO-NAS/DataLake")
#' db_list_public()
#' db_list_public(section = "Trade")
#'
#' # DuckLake mode - lists from master catalog
#' db_lake_connect_section("trade")
#' db_list_public()
#' db_list_public(section = "trade")
#' }
#' @export
db_list_public <- function(section = NULL) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")

  if (curr_mode == "hive") {
    .db_list_public_hive(section)
  } else {
    .db_list_public_ducklake(section)
  }
}

#' List public datasets (hive mode)
#' @noRd
.db_list_public_hive <- function(section = NULL) {
  catalog_path <- .db_catalog_path()

  # Return empty if catalog doesn't exist
  if (!dir.exists(catalog_path)) {
    return(data.frame(
      section = character(),
      dataset = character(),
      description = character(),
      owner = character(),
      tags = character(),
      stringsAsFactors = FALSE
    ))
  }

  # Find all JSON files in catalog
  if (!is.null(section)) {
    section <- .db_validate_name(section, "section")
    pattern <- file.path(catalog_path, section, "*.json")
  } else {
    pattern <- file.path(catalog_path, "*", "*.json")
  }

  files <- Sys.glob(pattern)

  if (length(files) == 0) {
    return(data.frame(
      section = character(),
      dataset = character(),
      description = character(),
      owner = character(),
      tags = character(),
      stringsAsFactors = FALSE
    ))
  }

  # Read each metadata file
  results <- lapply(files, function(f) {
    meta <- tryCatch(
      jsonlite::fromJSON(f, simplifyVector = FALSE),
      error = function(e) list()
    )

    data.frame(
      section = meta$section %||% basename(dirname(f)),
      dataset = meta$dataset %||% tools::file_path_sans_ext(basename(f)),
      description = meta$description %||% NA_character_,
      owner = meta$owner %||% NA_character_,
      tags = paste(meta$tags %||% character(), collapse = ", "),
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, results)
}

#' List public tables (DuckLake mode - from master catalog)
#' @noRd
.db_list_public_ducklake <- function(section = NULL) {
  master_con <- tryCatch(.db_master_connect(), error = function(e) NULL)
  if (is.null(master_con)) {
    return(data.frame(
      section = character(),
      schema = character(),
      table = character(),
      description = character(),
      owner = character(),
      tags = character(),
      row_count = integer(),
      last_updated = character(),
      stringsAsFactors = FALSE
    ))
  }
  on.exit(DBI::dbDisconnect(master_con))

  .db_ensure_master_schema(master_con)

  if (!is.null(section)) {
    section <- .db_validate_name(section, "section")
    result <- DBI::dbGetQuery(master_con, "
      SELECT section_name as section, schema_name as schema, table_name as 'table',
             description, owner, tags, row_count, last_updated
      FROM tables
      WHERE section_name = ?
      ORDER BY section_name, schema_name, table_name
    ", params = list(section))
  } else {
    result <- DBI::dbGetQuery(master_con, "
      SELECT section_name as section, schema_name as schema, table_name as 'table',
             description, owner, tags, row_count, last_updated
      FROM tables
      ORDER BY section_name, schema_name, table_name
    ")
  }

  if (nrow(result) == 0) {
    return(data.frame(
      section = character(),
      schema = character(),
      table = character(),
      description = character(),
      owner = character(),
      tags = character(),
      row_count = integer(),
      last_updated = character(),
      stringsAsFactors = FALSE
    ))
  }

  result
}


#' Sync the public catalog with source metadata
#'
#' @description Scans the public catalog and updates entries from their source
#' metadata. Optionally removes entries where the source no longer exists.
#'
#' @param remove_orphans Logical. If TRUE, remove catalog entries where the
#'   source no longer exists. Default FALSE.
#' @return Invisibly returns a list with counts of synced, removed, and errors
#'
#' @examples
#' \dontrun{
#' # Hive mode
#' db_connect("//CSO-NAS/DataLake")
#' db_sync_catalog()
#' db_sync_catalog(remove_orphans = TRUE)
#'
#' # DuckLake mode
#' db_lake_connect_section("trade")
#' db_sync_catalog()
#' }
#' @export
db_sync_catalog <- function(remove_orphans = FALSE) {
  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() or db_lake_connect() first.", call. = FALSE)
  }

  curr_mode <- .db_get("mode")

  if (curr_mode == "hive") {
    .db_sync_catalog_hive(remove_orphans)
  } else {
    .db_sync_catalog_ducklake(remove_orphans)
  }
}

#' Sync public catalog (hive mode)
#' @noRd
.db_sync_catalog_hive <- function(remove_orphans = FALSE) {
  catalog_path <- .db_catalog_path()

  if (!dir.exists(catalog_path)) {
    message("No public catalog exists yet.")
    return(invisible(list(synced = 0, removed = 0, errors = 0)))
  }

  files <- Sys.glob(file.path(catalog_path, "*", "*.json"))

  synced <- 0
  removed <- 0
  errors <- 0

  for (f in files) {
    section <- basename(dirname(f))
    dataset <- tools::file_path_sans_ext(basename(f))

    # Check if source exists
    source_path <- .db_metadata_path(section, dataset)

    if (!file.exists(source_path)) {
      if (remove_orphans) {
        file.remove(f)
        message("Removed orphan: ", section, "/", dataset)
        removed <- removed + 1
      } else {
        message("Orphan found (source missing): ", section, "/", dataset)
      }
    } else {
      # Re-sync from source
      tryCatch({
        .db_publish_metadata(section, dataset)
        synced <- synced + 1
      }, error = function(e) {
        message("Error syncing ", section, "/", dataset, ": ", e$message)
        errors <<- errors + 1
      })
    }
  }

  message("Sync complete: ", synced, " synced, ", removed, " removed, ", errors, " errors")
  invisible(list(synced = synced, removed = removed, errors = errors))
}

#' Sync public catalog (DuckLake mode - current section only)
#' @noRd
.db_sync_catalog_ducklake <- function(remove_orphans = FALSE) {
  section <- .db_get("section")
  if (is.null(section)) {
    stop("No section set. Use db_lake_connect_section() first.", call. = FALSE)
  }

  con <- .db_get_con()
  catalog <- .db_get("catalog")

  master_con <- .db_master_connect()
  on.exit(DBI::dbDisconnect(master_con))

  .db_ensure_master_schema(master_con)

  # Get all public entries for this section
  public_entries <- DBI::dbGetQuery(master_con, "
    SELECT schema_name, table_name FROM tables WHERE section_name = ?
  ", params = list(section))

  synced <- 0
  removed <- 0
  errors <- 0

  for (i in seq_len(nrow(public_entries))) {
    schema <- public_entries$schema_name[i]
    tbl <- public_entries$table_name[i]

    # Check if table still exists
    table_exists <- tryCatch({
      result <- DBI::dbGetQuery(con, glue::glue("
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = {.db_sql_quote(catalog)}
          AND table_schema = {.db_sql_quote(schema)}
          AND table_name = {.db_sql_quote(tbl)}
        LIMIT 1
      "))
      nrow(result) > 0
    }, error = function(e) FALSE)

    if (!table_exists) {
      if (remove_orphans) {
        DBI::dbExecute(master_con, "
          DELETE FROM tables WHERE section_name = ? AND schema_name = ? AND table_name = ?
        ", params = list(section, schema, tbl))
        message("Removed orphan: ", schema, ".", tbl)
        removed <- removed + 1
      } else {
        message("Orphan found (table missing): ", schema, ".", tbl)
      }
    } else {
      # Re-sync
      tryCatch({
        .db_publish_to_master(schema, tbl)
        synced <- synced + 1
      }, error = function(e) {
        message("Error syncing ", schema, ".", tbl, ": ", e$message)
        errors <<- errors + 1
      })
    }
  }

  message("Sync complete: ", synced, " synced, ", removed, " removed, ", errors, " errors")
  invisible(list(synced = synced, removed = removed, errors = errors))
}


# ==============================================================================
# Master Discovery Catalog (DuckLake Multi-Catalog)
# ==============================================================================

#' Get path to the master discovery catalog
#' @noRd
.db_master_path <- function() {
  # Check for explicit option first
  master_path <- getOption("datapond.master_catalog")
  if (!is.null(master_path)) {
    return(master_path)
  }

  # Default: _master/discovery.sqlite relative to data_path parent
  data_path <- .db_get("data_path")
  if (is.null(data_path)) {
    stop("Not connected. Cannot determine master catalog path.", call. = FALSE)
  }

  file.path(dirname(data_path), "_master", "discovery.sqlite")
}

#' Connect to master discovery catalog
#' @noRd
.db_master_connect <- function(master_path = NULL) {
  if (is.null(master_path)) {
    master_path <- .db_master_path()
  }

  # Ensure directory exists
  dir.create(dirname(master_path), recursive = TRUE, showWarnings = FALSE)

  DBI::dbConnect(RSQLite::SQLite(), master_path)
}

#' Ensure master catalog schema exists
#' @noRd
.db_ensure_master_schema <- function(master_con) {
  # Create sections table
  DBI::dbExecute(master_con, "
    CREATE TABLE IF NOT EXISTS sections (
      section_name TEXT PRIMARY KEY,
      catalog_path TEXT NOT NULL,
      data_path TEXT NOT NULL,
      description TEXT,
      owner TEXT,
      registered_at TEXT DEFAULT (datetime('now'))
    )
  ")

  # Create tables registry
  DBI::dbExecute(master_con, "
    CREATE TABLE IF NOT EXISTS tables (
      section_name TEXT NOT NULL,
      schema_name TEXT NOT NULL,
      table_name TEXT NOT NULL,
      description TEXT,
      owner TEXT,
      tags TEXT,
      columns_json TEXT,
      row_count INTEGER,
      last_updated TEXT,
      synced_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (section_name, schema_name, table_name),
      FOREIGN KEY (section_name) REFERENCES sections(section_name)
    )
  ")

  invisible(TRUE)
}

#' Set up the master discovery catalog
#'
#' @description Creates the master discovery catalog SQLite database with the
#' required schema. This is a one-time admin task.
#'
#' @param master_path Path to the master catalog SQLite file
#' @return Invisibly returns the master_path
#'
#' @examples
#' \dontrun{
#' db_setup_master("//CSO-NAS/DataLake/_master/discovery.sqlite")
#' }
#' @export
db_setup_master <- function(master_path) {
  if (missing(master_path)) {
    stop("master_path is required.", call. = FALSE)
  }

  # Ensure directory exists
  dir.create(dirname(master_path), recursive = TRUE, showWarnings = FALSE)

  master_con <- DBI::dbConnect(RSQLite::SQLite(), master_path)
  on.exit(DBI::dbDisconnect(master_con))

  .db_ensure_master_schema(master_con)

  message("Master discovery catalog created at: ", master_path)
  invisible(master_path)
}

#' Register a section in the master catalog
#'
#' @description Registers a DuckLake section (with its own catalog) in the
#' master discovery catalog, making it discoverable organisation-wide.
#'
#' @param section Section name (e.g., "trade", "labour")
#' @param catalog_path Path to the section's DuckLake catalog file
#' @param data_path Path to the section's data folder
#' @param description Optional description of the section
#' @param owner Optional owner/team name
#' @param master_path Path to master catalog (uses default if not specified)
#' @return Invisibly returns TRUE
#'
#' @examples
#' \dontrun{
#' db_register_section(
#'   section = "trade",
#'   catalog_path = "//CSO-NAS/DataLake/trade/catalog.sqlite",
#'   data_path = "//CSO-NAS/DataLake/trade/data",
#'   owner = "Trade Team"
#' )
#' }
#' @export
db_register_section <- function(section, catalog_path, data_path,
                                 description = NULL, owner = NULL,
                                 master_path = NULL) {
  section <- .db_validate_name(section, "section")

  master_con <- .db_master_connect(master_path)
  on.exit(DBI::dbDisconnect(master_con))

  .db_ensure_master_schema(master_con)

  # Upsert section (convert NULL to NA for RSQLite compatibility)
  DBI::dbExecute(master_con, "
    INSERT OR REPLACE INTO sections
    (section_name, catalog_path, data_path, description, owner, registered_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
  ", params = list(
    section, catalog_path, data_path,
    if (is.null(description)) NA_character_ else description,
    if (is.null(owner)) NA_character_ else owner
  ))

  message("Registered section '", section, "' in master catalog")
  invisible(TRUE)
}

#' Unregister a section from the master catalog
#'
#' @description Removes a section from the master discovery catalog.
#' This does not delete the section's data or catalog.
#'
#' @param section Section name
#' @param master_path Path to master catalog (uses default if not specified)
#' @return Invisibly returns TRUE
#'
#' @examples
#' \dontrun{
#' db_unregister_section("trade")
#' }
#' @export
db_unregister_section <- function(section, master_path = NULL) {
  section <- .db_validate_name(section, "section")

  master_con <- .db_master_connect(master_path)
  on.exit(DBI::dbDisconnect(master_con))

  # Remove section and its tables
DBI::dbExecute(master_con, "DELETE FROM tables WHERE section_name = ?",
                 params = list(section))
  DBI::dbExecute(master_con, "DELETE FROM sections WHERE section_name = ?",
                 params = list(section))

  message("Unregistered section '", section, "' from master catalog")
  invisible(TRUE)
}

#' List registered sections
#'
#' @description Lists all sections registered in the master discovery catalog.
#'
#' @param master_path Path to master catalog (uses default if not specified)
#' @return A data.frame with section information
#'
#' @examples
#' \dontrun{
#' db_list_registered_sections()
#' }
#' @export
db_list_registered_sections <- function(master_path = NULL) {
  master_con <- .db_master_connect(master_path)
  on.exit(DBI::dbDisconnect(master_con))

  .db_ensure_master_schema(master_con)

  DBI::dbGetQuery(master_con, "
    SELECT section_name, catalog_path, data_path, description, owner, registered_at
    FROM sections
    ORDER BY section_name
  ")
}

#' Publish table metadata to master catalog (DuckLake mode)
#' @noRd
.db_publish_to_master <- function(schema, table) {
  section <- .db_get("section")
  if (is.null(section)) {
    stop("No section set. Use db_lake_connect_section() or set section manually.",
         call. = FALSE)
  }

  master_con <- .db_master_connect()
  on.exit(DBI::dbDisconnect(master_con))

  .db_ensure_master_schema(master_con)

  # Get column info from DuckLake catalog
  con <- .db_get_con()
  catalog <- .db_get("catalog")

  cols <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_catalog = {.db_sql_quote(catalog)}
        AND table_schema = {.db_sql_quote(schema)}
        AND table_name = {.db_sql_quote(table)}
      ORDER BY ordinal_position
    "))
  }, error = function(e) data.frame())

  # Get docs
  docs <- tryCatch(db_get_docs(schema = schema, table = table),
                   error = function(e) list())

  # Get row count
  row_count <- tryCatch({
    DBI::dbGetQuery(con, glue::glue(
      "SELECT COUNT(*) as n FROM {catalog}.{schema}.{table}"
    ))$n[1]
  }, error = function(e) NA_integer_)

  # Upsert to master (convert NULL to NA for RSQLite compatibility)
  DBI::dbExecute(master_con, "
    INSERT OR REPLACE INTO tables
    (section_name, schema_name, table_name, description, owner, tags,
     columns_json, row_count, last_updated, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
  ", params = list(
    section, schema, table,
    if (is.null(docs$description)) NA_character_ else docs$description,
    if (is.null(docs$owner)) NA_character_ else docs$owner,
    if (!is.null(docs$tags) && length(docs$tags) > 0) paste(docs$tags, collapse = ",") else NA_character_,
    if (nrow(cols) > 0) as.character(jsonlite::toJSON(cols)) else NA_character_,
    if (is.na(row_count)) NA_integer_ else as.integer(row_count),
    format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  ))

  invisible(TRUE)
}

#' Remove table from master catalog (DuckLake mode)
#' @noRd
.db_unpublish_from_master <- function(schema, table) {
  section <- .db_get("section")
  if (is.null(section)) {
    stop("No section set. Use db_lake_connect_section() or set section manually.",
         call. = FALSE)
  }

  master_con <- .db_master_connect()
  on.exit(DBI::dbDisconnect(master_con))

  DBI::dbExecute(master_con, "
    DELETE FROM tables
    WHERE section_name = ? AND schema_name = ? AND table_name = ?
  ", params = list(section, schema, table))

  invisible(TRUE)
}

#' Check if table is public in master catalog (DuckLake mode)
#' @noRd
.db_is_public_in_master <- function(schema, table) {
  section <- .db_get("section")
  if (is.null(section)) {
    return(FALSE)
  }

  master_con <- tryCatch(.db_master_connect(), error = function(e) NULL)
  if (is.null(master_con)) {
    return(FALSE)
  }
  on.exit(DBI::dbDisconnect(master_con))

  result <- tryCatch({
    DBI::dbGetQuery(master_con, "
      SELECT 1 FROM tables
      WHERE section_name = ? AND schema_name = ? AND table_name = ?
      LIMIT 1
    ", params = list(section, schema, table))
  }, error = function(e) data.frame())

  nrow(result) > 0
}


# ==============================================================================
# Metadata Table Setup (DuckLake)
# ==============================================================================

#' Ensure metadata schema and tables exist
#' @noRd
.db_ensure_metadata_table <- function(con, catalog) {
  # Check if _metadata schema exists
  schema_exists <- tryCatch({
    res <- DBI::dbGetQuery(con, glue::glue("
      SELECT 1 FROM information_schema.schemata 
      WHERE catalog_name = {.db_sql_quote(catalog)} 
        AND schema_name = '_metadata'
    "))
    nrow(res) > 0
  }, error = function(e) FALSE)
  

  if (!schema_exists) {
    DBI::dbExecute(con, glue::glue("CREATE SCHEMA {catalog}._metadata"))
  }
  
  # Check if table_docs exists
  table_exists <- tryCatch({
    res <- DBI::dbGetQuery(con, glue::glue("
      SELECT 1 FROM information_schema.tables 
      WHERE table_catalog = {.db_sql_quote(catalog)} 
        AND table_schema = '_metadata'
        AND table_name = 'table_docs'
    "))
    nrow(res) > 0
  }, error = function(e) FALSE)
  
  if (!table_exists) {
    DBI::dbExecute(con, glue::glue("
      CREATE TABLE {catalog}._metadata.table_docs (
        schema_name VARCHAR NOT NULL,
        table_name VARCHAR NOT NULL,
        description VARCHAR,
        owner VARCHAR,
        tags VARCHAR,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
      )
    "))
  }
  
  # Check if column_docs exists
  col_table_exists <- tryCatch({
    res <- DBI::dbGetQuery(con, glue::glue("
      SELECT 1 FROM information_schema.tables 
      WHERE table_catalog = {.db_sql_quote(catalog)} 
        AND table_schema = '_metadata'
        AND table_name = 'column_docs'
    "))
    nrow(res) > 0
  }, error = function(e) FALSE)
  
  if (!col_table_exists) {
    DBI::dbExecute(con, glue::glue("
      CREATE TABLE {catalog}._metadata.column_docs (
        schema_name VARCHAR NOT NULL,
        table_name VARCHAR NOT NULL,
        column_name VARCHAR NOT NULL,
        description VARCHAR,
        units VARCHAR,
        notes VARCHAR,
        updated_at TIMESTAMP
      )
    "))
  }
  
  invisible(TRUE)
}
