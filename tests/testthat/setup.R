# tests/testthat/setup.R
# Shared setup for all tests

# ==============================================================================
# Version Requirements
# ==============================================================================

# Minimum required versions for full functionality
MIN_DUCKLAKE_VERSION <- "1.0.0"
MIN_DUCKDB_VERSION <- "1.5.2"

# ==============================================================================
# Test Helpers
# ==============================================================================

#' Clean up the package's internal connection state
#'
#' This should be called between tests to ensure isolation
clean_db_env <- function() {
  env <- datapond:::.db_env

  # Disconnect if connected
  tryCatch({
    if (exists("con", envir = env, inherits = FALSE) &&
        DBI::dbIsValid(env$con)) {
      DBI::dbDisconnect(env$con, shutdown = TRUE)
    }
  }, error = function(e) NULL)

  # Clear all state
  rm(list = ls(envir = env), envir = env)


  invisible(TRUE)
}

#' Check if DuckLake extension is available
ducklake_available <- function() {
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    DBI::dbExecute(con, "INSTALL ducklake")
    DBI::dbExecute(con, "LOAD ducklake")
    TRUE
  }, error = function(e) FALSE)
}

#' Get the installed DuckLake extension version
ducklake_version <- function() {
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    DBI::dbExecute(con, "INSTALL ducklake")
    DBI::dbExecute(con, "LOAD ducklake")
    result <- DBI::dbGetQuery(con,
      "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
    )
    if (nrow(result) > 0) result$extension_version[1] else NA_character_
  }, error = function(e) NA_character_)
}

#' Get the installed DuckDB version
duckdb_version <- function() {
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    result <- DBI::dbGetQuery(con, "SELECT version() AS v")
    # Extract version number (e.g., "v1.5.2" -> "1.5.2")
    gsub("^v", "", result$v[1])
  }, error = function(e) NA_character_)
}

#' Check if DuckLake version meets minimum requirement
ducklake_version_ok <- function(min_version = MIN_DUCKLAKE_VERSION) {
  ver <- ducklake_version()
  if (is.na(ver)) return(FALSE)
  utils::compareVersion(ver, min_version) >= 0
}

#' Skip test if DuckLake version is below minimum
skip_if_ducklake_below <- function(min_version) {
  ver <- ducklake_version()
  if (is.na(ver) || utils::compareVersion(ver, min_version) < 0) {
    testthat::skip(paste0("Requires DuckLake >= ", min_version, " (have: ", ver, ")"))
  }
}

#' Create a temporary directory for test data
#'
#' @return List with temp_dir, metadata_path, and data_path
create_test_lake <- function(prefix = "test") {
  temp_dir <- tempfile(pattern = paste0(prefix, "_lake_"))
  dir.create(temp_dir, recursive = TRUE)

  list(
    temp_dir = temp_dir,
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    sqlite_path = file.path(temp_dir, "catalog.sqlite"),
    data_path = file.path(temp_dir, "data")
  )
}

#' Clean up a test lake directory
cleanup_test_lake <- function(lake) {
  if (!is.null(lake$temp_dir) && dir.exists(lake$temp_dir)) {
    unlink(lake$temp_dir, recursive = TRUE)
  }
}

#' Skip test if DuckLake is not available
skip_without_ducklake <- function() {
  if (!ducklake_available()) {
    testthat::skip("DuckLake extension not available")
  }
}

# ==============================================================================
# Global Teardown
# ==============================================================================

# Ensure cleanup after all tests
withr::defer(clean_db_env(), teardown_env())
