# R/connection.R

.db_env <- new.env(parent = emptyenv())

# internal: validate "simple" names to avoid traversal like ../
.db_validate_name <- function(x, arg = deparse(substitute(x))) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    stop(arg, " must be a single, non-empty string.", call. = FALSE)
  }
  # allow letters, numbers, underscore, dash; adjust if you need spaces
  if (!grepl("^[A-Za-z0-9_-]+$", x)) {
    stop(
      arg, " contains invalid characters. Allowed: A-Z a-z 0-9 _ -",
      call. = FALSE
    )
  }
  x
}

# internal: ensure a connection exists & is valid
.db_get_con <- function() {
  if (exists("con", envir = .db_env, inherits = FALSE) &&
      DBI::dbIsValid(.db_env$con)) {
    return(.db_env$con)
  }
  NULL
}

# internal: SQL-quote a string literal
.db_sql_quote <- function(x) paste0("'", gsub("'", "''", x), "'")

# internal: get value from .db_env or return default
.db_get <- function(name, default = NULL) {
  if (exists(name, envir = .db_env, inherits = FALSE)) {
    get(name, envir = .db_env, inherits = FALSE)
  } else {
    default
  }
}

# internal: build the DuckLake connection string based on catalog type
.db_build_ducklake_dsn <- function(catalog_type, metadata_path) {
  switch(catalog_type,
    duckdb = paste0("ducklake:", metadata_path),
    sqlite = paste0("ducklake:sqlite:", metadata_path),
    postgres = paste0("ducklake:postgres:", metadata_path),
    stop("Unknown catalog_type: ", catalog_type, call. = FALSE)
  )
}

# internal: load required extensions for catalog type
.db_load_catalog_extensions <- function(con, catalog_type) {
  # Always need ducklake
  try(DBI::dbExecute(con, "INSTALL ducklake"), silent = TRUE)
  DBI::dbExecute(con, "LOAD ducklake")
  
  # Load backend-specific extension
  if (catalog_type == "sqlite") {
    try(DBI::dbExecute(con, "INSTALL sqlite"), silent = TRUE)
    DBI::dbExecute(con, "LOAD sqlite")
  } else if (catalog_type == "postgres") {
    try(DBI::dbExecute(con, "INSTALL postgres"), silent = TRUE)
    DBI::dbExecute(con, "LOAD postgres")
  }
  # duckdb needs no extra extension
  
  invisible(TRUE)
}


#' Connect to the CSO hive parquet lake
#' @description Establishes a singleton connection to DuckDB and stores base path.
#' @param path Root path for the lake (e.g. "//CSO-NAS/DataLake")
#' @param db DuckDB database file path. Use ":memory:" for in-memory.
#' @param threads Number of DuckDB threads (NULL leaves default)
#' @param memory_limit e.g. "4GB" (NULL leaves default)
#' @param load_extensions character vector of extensions to install/load, e.g. c("httpfs")
#' @return DuckDB connection object
#' @examples
#' \dontrun{
#' # Connect to hive-partitioned data lake
#' db_connect(path = "//CSO-NAS/DataLake")
#' 
#' # With performance tuning
#' db_connect(path = "//CSO-NAS/DataLake", threads = 4, memory_limit = "8GB")
#' }
#' @export
db_connect <- function(path = "//CSO-NAS/DataLake",
                        db = ":memory:",
                        threads = NULL,
                        memory_limit = NULL,
                        load_extensions = NULL) {
  con <- .db_get_con()
  if (!is.null(con)) {
    # Check if already connected in same mode
    curr_mode <- .db_get("mode")
    if (identical(curr_mode, "hive")) {
      return(con)
    } else {
      # Connected in different mode - disconnect first
      message("Disconnecting from DuckLake mode to connect in hive mode...")
      db_disconnect()
    }
  }

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db)

  # Optional tuning
  if (!is.null(threads)) {
    DBI::dbExecute(con, sprintf("SET threads=%d", as.integer(threads)))
  }
  if (!is.null(memory_limit)) {
    DBI::dbExecute(con, sprintf("SET memory_limit='%s'", gsub("'", "''", memory_limit)))
  }

  # Optional extensions (for cloud/remote FS etc.)
  if (!is.null(load_extensions)) {
    for (ext in load_extensions) {
      ext_clean <- .db_validate_name(ext, "extension")
      try(DBI::dbExecute(con, sprintf("INSTALL %s", ext_clean)), silent = TRUE)
      DBI::dbExecute(con, sprintf("LOAD %s", ext_clean))
    }
  }

  assign("data_path", path, envir = .db_env)
  assign("db_path", db, envir = .db_env)
  assign("con", con, envir = .db_env)
  assign("mode", "hive", envir = .db_env)

  # Clean up automatically when session ends (best effort)
  reg.finalizer(.db_env, function(e) {
    if (exists("con", envir = e, inherits = FALSE)) {
      try(DBI::dbDisconnect(e$con, shutdown = TRUE), silent = TRUE)
    }
  }, onexit = TRUE)

  con
}


#' Connect to DuckDB + attach a DuckLake catalog
#'
#' @param duckdb_db DuckDB database file path. Use ":memory:" for in-memory.
#' @param catalog DuckLake catalog name inside DuckDB (e.g. "cso")
#' @param catalog_type Type of catalog database backend. One of:
#'   \itemize{
#'     \item "duckdb" (default): Single-client local use. Metadata stored in .ducklake file.
#'     \item "sqlite": Multi-client local use. Metadata stored in .sqlite file. 
#'       Supports multiple readers + single writer with automatic retry.
#'       Recommended for most CSO use cases with shared network drives.
#'     \item "postgres": Multi-user lakehouse. Metadata stored in PostgreSQL database.
#'       Requires PostgreSQL 12+ and connection string in metadata_path.
#'   }
#' @param metadata_path Path or connection string for DuckLake metadata:
#'   \itemize{
#'     \item For "duckdb": file path (e.g. "metadata.ducklake")
#'     \item For "sqlite": file path (e.g. "//CSO-NAS/DataLake/catalog.sqlite")
#'     \item For "postgres": connection string (e.g. "dbname=ducklake_catalog host=localhost")
#'   }
#' @param data_path Root storage path where DuckLake writes Parquet data files
#' @param snapshot_version Optional integer snapshot version to attach at
#' @param snapshot_time Optional timestamp string to attach at (e.g. "2025-05-26 00:00:00")
#' @param threads Number of DuckDB threads (NULL leaves default)
#' @param memory_limit e.g. "4GB" (NULL leaves default)
#' @param load_extensions character vector of extensions to install/load, e.g. c("httpfs")
#' @return DuckDB connection object
#' @examples
#' \dontrun{
#' # DuckDB catalog (single user, simplest setup)
#' db_lake_connect(
#'   metadata_path = "metadata.ducklake",
#'   data_path = "//CSO-NAS/DataLake"
#' )
#' 
#' # SQLite catalog (multiple local users - RECOMMENDED for shared drives)
#' db_lake_connect(
#'   catalog_type = "sqlite",
#'   metadata_path = "//CSO-NAS/DataLake/catalog.sqlite",
#'   data_path = "//CSO-NAS/DataLake/data"
#' )
#' 
#' # PostgreSQL catalog (multi-user lakehouse, remote clients)
#' db_lake_connect(
#'   catalog_type = "postgres",
#'   metadata_path = "dbname=ducklake_catalog host=db.cso.ie user=analyst",
#'   data_path = "//CSO-NAS/DataLake/data"
#' )
#' 
#' # Time travel - connect to a specific snapshot
#' db_lake_connect(
#'   catalog_type = "sqlite",
#'   metadata_path = "catalog.sqlite",
#'   data_path = "//CSO-NAS/DataLake/data",
#'   snapshot_version = 5
#' )
#' }
#' @export
db_lake_connect <- function(duckdb_db = ":memory:",
                       catalog = "cso",
                       catalog_type = c("duckdb", "sqlite", "postgres"),
                       metadata_path = "metadata.ducklake",
                       data_path = "//CSO-NAS/DataLake",
                       snapshot_version = NULL,
                       snapshot_time = NULL,
                       threads = NULL,
                       memory_limit = NULL,
                       load_extensions = NULL) {
  
  con <- .db_get_con()
  if (!is.null(con)) {
    # Check if already connected in same mode
    curr_mode <- .db_get("mode")
    if (identical(curr_mode, "ducklake")) {
      return(con)
    } else {
      # Connected in different mode - disconnect first
      message("Disconnecting from hive mode to connect in DuckLake mode...")
      db_disconnect()
    }
  }
  
  catalog_type <- match.arg(catalog_type)
  
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = duckdb_db)
  
  # Optional tuning
  if (!is.null(threads)) {
    DBI::dbExecute(con, sprintf("SET threads=%d", as.integer(threads)))
  }
  if (!is.null(memory_limit)) {
    DBI::dbExecute(con, sprintf("SET memory_limit='%s'", gsub("'", "''", memory_limit)))
  }
  
  # Optional extensions (for cloud/remote FS etc.)
  if (!is.null(load_extensions)) {
    for (ext in load_extensions) {
      ext_clean <- .db_validate_name(ext, "extension")
      try(DBI::dbExecute(con, sprintf("INSTALL %s", ext_clean)), silent = TRUE)
      DBI::dbExecute(con, sprintf("LOAD %s", ext_clean))
    }
  }

  catalog <- .db_validate_name(catalog, "catalog")

  # Load DuckLake and any required backend extensions
  .db_load_catalog_extensions(con, catalog_type)

  # Build the DuckLake connection string
  ducklake_dsn <- .db_build_ducklake_dsn(catalog_type, metadata_path)

  # Build ATTACH options
  attach_opts <- c(glue::glue("DATA_PATH {.db_sql_quote(data_path)}"))

  if (!is.null(snapshot_version)) {
    attach_opts <- c(attach_opts, glue::glue("SNAPSHOT_VERSION {as.integer(snapshot_version)}"))
  }
  if (!is.null(snapshot_time)) {
    attach_opts <- c(attach_opts, glue::glue("SNAPSHOT_TIME {.db_sql_quote(snapshot_time)}"))
  }

  attach_sql <- glue::glue(
    "ATTACH {.db_sql_quote(ducklake_dsn)} AS {catalog} ({paste(attach_opts, collapse = ', ')})"
  )

  tryCatch({
    DBI::dbExecute(con, attach_sql)
  }, error = function(e) {
    DBI::dbDisconnect(con, shutdown = TRUE)
    
    # Provide helpful error messages based on catalog type
    hint <- switch(catalog_type,
      sqlite = "Ensure the sqlite extension is available and the metadata file path is accessible.",
      postgres = "Ensure PostgreSQL is running, the database exists, and the connection string is correct.",
      duckdb = "Ensure the metadata file path is accessible."
    )
    stop("Failed to attach DuckLake catalog.\n", hint, "\n\nOriginal error: ", e$message, call. = FALSE)
  })
  
  DBI::dbExecute(con, glue::glue("USE {catalog}"))

  assign("con", con, envir = .db_env)
  assign("catalog", catalog, envir = .db_env)
  assign("catalog_type", catalog_type, envir = .db_env)
  assign("metadata_path", metadata_path, envir = .db_env)
  assign("data_path", data_path, envir = .db_env)
  assign("db_path", duckdb_db, envir = .db_env)
  assign("mode", "ducklake", envir = .db_env)
  assign("snapshot_version", snapshot_version, envir = .db_env)
  assign("snapshot_time", snapshot_time, envir = .db_env)

  # Clean up automatically when session ends (best effort)
  reg.finalizer(.db_env, function(e) {
    if (exists("con", envir = e, inherits = FALSE)) {
      try(DBI::dbDisconnect(e$con, shutdown = TRUE), silent = TRUE)
    }
  }, onexit = TRUE)

  con
}


#' Get connection status and configuration
#' 
#' @description Returns information about the current connection state,
#' including mode (hive/ducklake), paths, and connection validity.
#' @param verbose If TRUE, prints a formatted summary. If FALSE, returns a list silently.
#' @return A list (invisibly if verbose=TRUE) containing connection details.
#' @examples
#' \dontrun{
#' db_lake_connect()
#' db_status()
#' }
#' @export
db_status <- function(verbose = TRUE) {
  con <- .db_get_con()
  connected <- !is.null(con)

  status <- list(
    connected = connected,
    mode = .db_get("mode", NA_character_),
    data_path = .db_get("data_path", NA_character_),
    db_path = .db_get("db_path", NA_character_),
    catalog = .db_get("catalog", NA_character_),
    catalog_type = .db_get("catalog_type", NA_character_),
    metadata_path = .db_get("metadata_path", NA_character_),
    snapshot_version = .db_get("snapshot_version", NA_integer_),
    snapshot_time = .db_get("snapshot_time", NA_character_)
  )
  
  if (verbose) {
    cat("\n")
    cat("-- CSO Data Lake Connection Status ", strrep("-", 30), "\n", sep = "")
    cat("\n")
    
    if (connected) {
      cat("[x] Connected\n\n")
      cat("  Mode:         ", status$mode, "\n")
      cat("  Data Path:    ", status$data_path, "\n")
      cat("  DuckDB Path:  ", status$db_path, "\n")
      
      if (status$mode == "ducklake") {
        cat("\n")
        cat("-- DuckLake Configuration ", strrep("-", 20), "\n", sep = "")
        cat("  Catalog:       ", status$catalog, "\n")
        cat("  Catalog Type:  ", status$catalog_type, "\n")
        cat("  Metadata Path: ", status$metadata_path, "\n")
        
        # Show concurrency info based on catalog type
        concurrency_info <- switch(status$catalog_type,
          duckdb = "(single client only)",
          sqlite = "(multi-read, single-write with retry)",
          postgres = "(full concurrent access)",
          ""
        )
        if (nzchar(concurrency_info)) {
          cat("  Concurrency:   ", concurrency_info, "\n")
        }
        
        has_version <- !is.null(status$snapshot_version) && !is.na(status$snapshot_version)
        has_time <- !is.null(status$snapshot_time) && !is.na(status$snapshot_time)
        
        if (has_version || has_time) {
          cat("\n")
          cat("-- Time Travel ", strrep("-", 30), "\n", sep = "")
          if (has_version) {
            cat("  Snapshot Version: ", status$snapshot_version, "\n")
          }
          if (has_time) {
            cat("  Snapshot Time:    ", status$snapshot_time, "\n")
          }
        }
      }
    } else {
      cat("[ ] Not connected\n\n")
      cat("Use db_connect() for hive mode or db_lake_connect() for DuckLake mode.\n")
    }
    cat("\n")
    
    invisible(status)
  } else {
    status
  }
}


#' Disconnect from the CSO Data Lake
#' @return Invisibly returns TRUE if disconnected, FALSE if was not connected.
#' @examples
#' \dontrun{
#' db_connect()
#' # ... do work ...
#' db_disconnect()
#' }
#' @export
db_disconnect <- function() {
  con <- .db_get_con()
  if (is.null(con)) return(invisible(FALSE))
  DBI::dbDisconnect(con, shutdown = TRUE)

  # Clear all stored state
  rm(list = ls(envir = .db_env), envir = .db_env)

  invisible(TRUE)
}


