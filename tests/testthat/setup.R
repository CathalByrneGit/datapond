# tests/testthat/setup.R
# Shared setup for all tests

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
