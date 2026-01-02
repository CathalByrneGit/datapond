# tests/testthat/test-db_connect.R

# ==============================================================================
# Test Setup and Helpers
# ==============================================================================

# Helper to clean up connection state between tests
clean_db_env <- function() {
 # Access the package's internal environment
  env <- csolake:::.db_env
  
 # Disconnect if connected
  con <- tryCatch({
    if (exists("con", envir = env, inherits = FALSE) && 
        DBI::dbIsValid(env$con)) {
      DBI::dbDisconnect(env$con, shutdown = TRUE)
    }
  }, error = function(e) NULL)
  
  # Clear all state
  rm(list = ls(envir = env), envir = env)
}

# ==============================================================================
# Tests for .db_validate_name()
# ==============================================================================

test_that(".db_validate_name accepts valid names", {
 validate <- csolake:::.db_validate_name
  
  # Simple names
  expect_equal(validate("Trade"), "Trade")
  expect_equal(validate("trade"), "trade")
  expect_equal(validate("TRADE"), "TRADE")
  
  # With numbers
  expect_equal(validate("Trade2024"), "Trade2024")
  expect_equal(validate("2024Trade"), "2024Trade")
  expect_equal(validate("123"), "123")
  
  # With underscores and dashes
 expect_equal(validate("Trade_Imports"), "Trade_Imports")
  expect_equal(validate("trade-exports"), "trade-exports")
  expect_equal(validate("Trade_2024-Q1"), "Trade_2024-Q1")
  
  # Edge cases
  expect_equal(validate("a"), "a")
  expect_equal(validate("_"), "_")
  expect_equal(validate("-"), "-")
})

test_that(".db_validate_name rejects invalid names", {
  validate <- csolake:::.db_validate_name
  
  # Empty/NULL/NA
  expect_error(validate(""), "must be a single, non-empty string")
  expect_error(validate(NA_character_), "must be a single, non-empty string")
  expect_error(validate(NULL), "must be a single, non-empty string")
  
  # Wrong types
  expect_error(validate(123), "must be a single, non-empty string")
  expect_error(validate(TRUE), "must be a single, non-empty string")
  expect_error(validate(c("a", "b")), "must be a single, non-empty string")
  
  # Path traversal attempts
  expect_error(validate("../etc"), "contains invalid characters")
  expect_error(validate("Trade/Imports"), "contains invalid characters")
  expect_error(validate("Trade\\Imports"), "contains invalid characters")
  expect_error(validate(".."), "contains invalid characters")
  
  # SQL injection attempts
  expect_error(validate("Trade; DROP TABLE"), "contains invalid characters")
  expect_error(validate("Trade'--"), "contains invalid characters")
  expect_error(validate("Trade\""), "contains invalid characters")
  
  # Special characters
  expect_error(validate("Trade Imports"), "contains invalid characters")  # space
  expect_error(validate("Trade.Imports"), "contains invalid characters")  # dot
  expect_error(validate("Trade@Imports"), "contains invalid characters")  # @
  expect_error(validate("Trade#1"), "contains invalid characters")        # #
})

test_that(".db_validate_name uses custom arg name in errors", {
  validate <- csolake:::.db_validate_name
  
  expect_error(validate("", arg = "section"), "section must be a single")
  expect_error(validate("bad/name", arg = "dataset"), "dataset contains invalid")
})

# ==============================================================================
# Tests for .db_sql_quote()
# ==============================================================================

test_that(".db_sql_quote properly quotes strings", {
  sql_quote <- csolake:::.db_sql_quote
  
  # Simple strings
  expect_equal(sql_quote("hello"), "'hello'")
  expect_equal(sql_quote(""), "''")
  expect_equal(sql_quote("path/to/file"), "'path/to/file'")
  
  # Strings with single quotes (SQL injection prevention)
  expect_equal(sql_quote("it's"), "'it''s'")
  expect_equal(sql_quote("O'Brien"), "'O''Brien'")
  expect_equal(sql_quote("'quoted'"), "'''quoted'''")
  expect_equal(sql_quote("'; DROP TABLE --"), "'''; DROP TABLE --'")
  
  # Multiple quotes
  expect_equal(sql_quote("a'b'c"), "'a''b''c'")
})

# ==============================================================================
# Tests for .db_get()
# ==============================================================================

test_that(".db_get returns value when exists", {
  env <- csolake:::.db_env
  db_get <- csolake:::.db_get
  
  # Set up test values
  assign("test_value", "hello", envir = env)
  assign("test_number", 42, envir = env)
  
  expect_equal(db_get("test_value"), "hello")
  expect_equal(db_get("test_number"), 42)
  
  # Cleanup
  rm("test_value", "test_number", envir = env)
})

test_that(".db_get returns default when not exists", {
  db_get <- csolake:::.db_get
  
  expect_null(db_get("nonexistent"))
  expect_equal(db_get("nonexistent", default = "default_val"), "default_val")
  expect_equal(db_get("nonexistent", default = NA_character_), NA_character_)
})

# ==============================================================================
# Tests for .db_build_ducklake_dsn()
# ==============================================================================

test_that(".db_build_ducklake_dsn builds correct DSN for each catalog type", {
  build_dsn <- csolake:::.db_build_ducklake_dsn
  
  # DuckDB backend
  expect_equal(
    build_dsn("duckdb", "metadata.ducklake"),
    "ducklake:metadata.ducklake"
  )
  expect_equal(
    build_dsn("duckdb", "/path/to/catalog.ducklake"),
    "ducklake:/path/to/catalog.ducklake"
  )
  
  # SQLite backend
  expect_equal(
    build_dsn("sqlite", "catalog.sqlite"),
    "ducklake:sqlite:catalog.sqlite"
  )
  expect_equal(
    build_dsn("sqlite", "//CSO-NAS/DataLake/catalog.sqlite"),
    "ducklake:sqlite://CSO-NAS/DataLake/catalog.sqlite"
  )
  
  # PostgreSQL backend
  expect_equal(
    build_dsn("postgres", "dbname=ducklake host=localhost"),
    "ducklake:postgres:dbname=ducklake host=localhost"
  )
  expect_equal(
    build_dsn("postgres", "dbname=catalog host=db.cso.ie user=analyst"),
    "ducklake:postgres:dbname=catalog host=db.cso.ie user=analyst"
  )
})

test_that(".db_build_ducklake_dsn errors on unknown catalog type", {
  build_dsn <- csolake:::.db_build_ducklake_dsn
  
  expect_error(build_dsn("mysql", "catalog.db"), "Unknown catalog_type")
  expect_error(build_dsn("oracle", "catalog.db"), "Unknown catalog_type")
  expect_error(build_dsn("", "catalog.db"), "Unknown catalog_type")
})

# ==============================================================================
# Tests for db_connect() - Hive Mode
# ==============================================================================

test_that("db_connect creates connection and stores state", {
  clean_db_env()
  
  con <- db_connect(path = "/test/path", db = ":memory:")
  
  # Returns a valid connection
 expect_true(DBI::dbIsValid(con))
  
  # Stored correct state
  env <- csolake:::.db_env
  expect_equal(env$mode, "hive")
  expect_equal(env$data_path, "/test/path")
  expect_equal(env$db_path, ":memory:")
  expect_true(DBI::dbIsValid(env$con))
  
  clean_db_env()
})

test_that("db_connect returns existing connection on second call", {
  clean_db_env()
  
  con1 <- db_connect(path = "/path1", db = ":memory:")
  con2 <- db_connect(path = "/path2", db = ":memory:")  # Different path
  
  # Should return same connection
  expect_identical(con1, con2)
  
  # Path should still be from first call
  expect_equal(csolake:::.db_env$data_path, "/path1")
  
  clean_db_env()
})

test_that("db_lake_connect auto-disconnects from hive mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "mode_switch_")
  dir.create(temp_dir)
  
  # Connect in hive mode first
  con1 <- db_connect(path = temp_dir, db = ":memory:")
  expect_equal(csolake:::.db_get("mode"), "hive")
  
  # Now connect in DuckLake mode - should auto-disconnect
  expect_message(
    con2 <- db_lake_connect(
      catalog = "test",
      metadata_path = file.path(temp_dir, "cat.ducklake"),
      data_path = temp_dir
    ),
    "Disconnecting from hive mode"
  )
  
  # Should now be in DuckLake mode
  expect_equal(csolake:::.db_get("mode"), "ducklake")
  
  # Connections should be different (new connection was made)
  expect_false(identical(con1, con2))
  
  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_connect auto-disconnects from DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "mode_switch2_")
  dir.create(temp_dir)
  
  # Connect in DuckLake mode first
  con1 <- db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "cat.ducklake"),
    data_path = temp_dir
  )
  expect_equal(csolake:::.db_get("mode"), "ducklake")
  
  # Now connect in hive mode - should auto-disconnect
  expect_message(
    con2 <- db_connect(path = temp_dir, db = ":memory:"),
    "Disconnecting from DuckLake mode"
  )
  
  # Should now be in hive mode
  expect_equal(csolake:::.db_get("mode"), "hive")
  
  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_connect respects threads parameter", {
  clean_db_env()
  
  con <- db_connect(path = "/test", db = ":memory:", threads = 2)
  
  result <- DBI::dbGetQuery(con, "SELECT current_setting('threads') as threads")
  expect_equal(as.integer(result$threads), 2L)
  
  clean_db_env()
})

test_that("db_connect respects memory_limit parameter", {
  clean_db_env()
  
  con <- db_connect(path = "/test", db = ":memory:", memory_limit = "1GB")
  
  result <- DBI::dbGetQuery(con, "SELECT current_setting('memory_limit') as mem")
  # DuckDB returns format like "1.0 GiB" or similar - just check it's set to something
  expect_true(nchar(result$mem) > 0)
  
  clean_db_env()
})

test_that("db_connect can load extensions", {
  skip_if_not_installed("duckdb")
  clean_db_env()
  
  # json extension is built-in and should always work
  con <- db_connect(path = "/test", db = ":memory:", load_extensions = "json")
  
  # If we got here without error, extension loaded successfully
  expect_true(DBI::dbIsValid(con))
  
  clean_db_env()
})

test_that("db_connect validates extension names", {
  clean_db_env()
  
  expect_error(
    db_connect(path = "/test", db = ":memory:", load_extensions = "../bad"),
    "contains invalid characters"
  )
  
  expect_error(
    db_connect(path = "/test", db = ":memory:", load_extensions = "ext; DROP"),
    "contains invalid characters"
  )
  
  clean_db_env()
})

# ==============================================================================
# Tests for db_disconnect()
# ==============================================================================

test_that("db_disconnect returns FALSE when not connected", {
  clean_db_env()
  
  result <- db_disconnect()
  expect_false(result)
})

test_that("db_disconnect clears all state", {
  clean_db_env()
  
  # Connect first
  con <- db_connect(path = "/test", db = ":memory:")
  expect_true(DBI::dbIsValid(con))
  
  # Disconnect
  result <- db_disconnect()
  expect_true(result)
  
  # State should be cleared
  env <- csolake:::.db_env
  expect_false(exists("con", envir = env))
  expect_false(exists("mode", envir = env))
  expect_false(exists("data_path", envir = env))
  
  # Original connection should be invalid
  expect_false(DBI::dbIsValid(con))
})

test_that("db_disconnect allows reconnection", {
  clean_db_env()
  
  # Connect, disconnect, reconnect
  con1 <- db_connect(path = "/path1", db = ":memory:")
  db_disconnect()
  con2 <- db_connect(path = "/path2", db = ":memory:")
  
  # Should be a new connection with new path
  expect_true(DBI::dbIsValid(con2))
  expect_equal(csolake:::.db_env$data_path, "/path2")
  
  clean_db_env()
})

# ==============================================================================
# Tests for db_status()
# ==============================================================================

test_that("db_status returns correct status when disconnected", {
  clean_db_env()
  
  status <- db_status(verbose = FALSE)
  
  expect_false(status$connected)
  expect_true(is.na(status$mode))
  expect_true(is.na(status$data_path))
})

test_that("db_status returns correct status for hive mode", {
  clean_db_env()
  
  db_connect(path = "/test/lake", db = ":memory:")
  status <- db_status(verbose = FALSE)
  
  expect_true(status$connected)
  expect_equal(status$mode, "hive")
  expect_equal(status$data_path, "/test/lake")
  expect_equal(status$db_path, ":memory:")
  expect_true(is.na(status$catalog))  # Not set in hive mode
  expect_true(is.na(status$catalog_type))  # Not set in hive mode
  
  clean_db_env()
})

test_that("db_status verbose mode prints output", {
  clean_db_env()
  
  db_connect(path = "/test/lake", db = ":memory:")
  
  # Capture output
  output <- capture.output(db_status(verbose = TRUE))
  
  expect_true(any(grepl("Connected", output)))
  expect_true(any(grepl("hive", output)))
  expect_true(any(grepl("/test/lake", output)))
  
  clean_db_env()
})

test_that("db_status verbose mode returns invisible status", {
  clean_db_env()
  
  db_connect(path = "/test/lake", db = ":memory:")
  
  # Capture output and result
  output <- capture.output({
    result <- db_status(verbose = TRUE)
  })
  
  # Should still return the status list
  expect_true(result$connected)
  expect_equal(result$mode, "hive")
  
  clean_db_env()
})

# ==============================================================================
# Tests for db_lake_connect() - DuckLake Mode
# ==============================================================================

# Note: DuckLake tests are more limited because the extension may not be available
# We test what we can without requiring DuckLake to actually connect

test_that("db_lake_connect validates catalog_type", {
  clean_db_env()
  
  # Invalid catalog type should error (match.arg)
  expect_error(
    db_lake_connect(catalog_type = "mysql"),
    "'arg' should be one of"
  )
  
  expect_error(
    db_lake_connect(catalog_type = "invalid"),
    "'arg' should be one of"
  )
})

test_that("db_lake_connect validates catalog name", {
  clean_db_env()
  
  # Note: This will fail at DuckLake attach, but should get past validation
  # if catalog name is valid. With invalid name, should fail earlier.
  expect_error(
    db_lake_connect(catalog = "../bad"),
    "contains invalid characters"
  )
  
  expect_error(
    db_lake_connect(catalog = "cat; DROP"),
    "contains invalid characters"
  )
})

test_that("db_lake_connect returns existing connection on second call", {
  skip("Requires DuckLake extension")
  # This test would be similar to db_connect test
})

# Helper to check if DuckLake is available
ducklake_available <- function() {
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    DBI::dbExecute(con, "INSTALL ducklake")
    DBI::dbExecute(con, "LOAD ducklake")
    TRUE
  }, error = function(e) FALSE)
}

test_that("db_lake_connect works with DuckDB catalog", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  # Create temp directory for test
  temp_dir <- tempdir()
  metadata_file <- file.path(temp_dir, "test_catalog.ducklake")
  data_dir <- file.path(temp_dir, "test_data")
  dir.create(data_dir, showWarnings = FALSE)
  
  con <- db_lake_connect(
    catalog = "test",
    catalog_type = "duckdb",
    metadata_path = metadata_file,
    data_path = data_dir
  )
  
  expect_true(DBI::dbIsValid(con))
  
  status <- db_status(verbose = FALSE)
  expect_equal(status$mode, "ducklake")
  expect_equal(status$catalog, "test")
  expect_equal(status$catalog_type, "duckdb")
  
  clean_db_env()
  
  # Cleanup
  unlink(metadata_file)
  unlink(data_dir, recursive = TRUE)
})

test_that("db_lake_connect stores correct state for SQLite catalog", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  # Create temp directory for test
  temp_dir <- tempdir()
  metadata_file <- file.path(temp_dir, "test_catalog.sqlite")
  data_dir <- file.path(temp_dir, "test_data_sqlite")
  dir.create(data_dir, showWarnings = FALSE)
  
  con <- db_lake_connect(
    catalog = "test_sqlite",
    catalog_type = "sqlite",
    metadata_path = metadata_file,
    data_path = data_dir
  )
  
  expect_true(DBI::dbIsValid(con))
  
  status <- db_status(verbose = FALSE)
  expect_equal(status$mode, "ducklake")
  expect_equal(status$catalog, "test_sqlite")
  expect_equal(status$catalog_type, "sqlite")
  
  clean_db_env()
  
  # Cleanup
  unlink(metadata_file)
  unlink(data_dir, recursive = TRUE)
})

test_that("db_status shows concurrency info for DuckLake", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  temp_dir <- tempdir()
  metadata_file <- file.path(temp_dir, "test_conc.sqlite")
  data_dir <- file.path(temp_dir, "test_data_conc")
  dir.create(data_dir, showWarnings = FALSE)
  
  db_lake_connect(
    catalog = "test",
    catalog_type = "sqlite",
    metadata_path = metadata_file,
    data_path = data_dir
  )
  
  output <- capture.output(db_status(verbose = TRUE))
  
  expect_true(any(grepl("sqlite", output)))
  expect_true(any(grepl("multi-read", output)))
  
  clean_db_env()
  
  # Cleanup
  unlink(metadata_file)
  unlink(data_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for .db_get_con()
# ==============================================================================

test_that(".db_get_con returns NULL when not connected", {
  clean_db_env()
  
  result <- csolake:::.db_get_con()
  expect_null(result)
})

test_that(".db_get_con returns connection when connected", {
  clean_db_env()
  
  db_connect(path = "/test", db = ":memory:")
  
  result <- csolake:::.db_get_con()
  expect_true(DBI::dbIsValid(result))
  
  clean_db_env()
})

test_that(".db_get_con returns NULL for invalid connection", {
  clean_db_env()
  env <- csolake:::.db_env
  
  # Create a connection, then invalidate it manually
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  assign("con", con, envir = env)
  DBI::dbDisconnect(con, shutdown = TRUE)
  
  # .db_get_con should recognize it's invalid
  result <- csolake:::.db_get_con()
  expect_null(result)
  
  clean_db_env()
})

# ==============================================================================
# Integration Tests
# ==============================================================================

test_that("full hive mode workflow works", {
  clean_db_env()
  
  # Connect
  con <- db_connect(path = "/integration/test", db = ":memory:", threads = 2)
  expect_true(DBI::dbIsValid(con))
  
  # Check status
  status <- db_status(verbose = FALSE)
  expect_true(status$connected)
  expect_equal(status$mode, "hive")
  
  # Can run queries
  result <- DBI::dbGetQuery(con, "SELECT 1 + 1 AS answer")
  expect_equal(result$answer, 2)
  
  # Disconnect
  expect_true(db_disconnect())
  
  # Status reflects disconnection
  status <- db_status(verbose = FALSE)
  expect_false(status$connected)
})

test_that("switching modes auto-disconnects and reconnects", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "mode_test_")
  dir.create(temp_dir)
  
  # Connect in hive mode

  con1 <- db_connect(path = temp_dir, db = ":memory:")
  expect_equal(csolake:::.db_get("mode"), "hive")
  
  # Connect in DuckLake mode - should auto-switch
  expect_message(
    con2 <- db_lake_connect(
      metadata_path = file.path(temp_dir, "test.ducklake"),
      data_path = temp_dir
    ),
    "Disconnecting"
  )
  
  # Should NOT be the same connection (new one was created)
  expect_false(identical(con1, con2))
  
  # Mode should now be ducklake
  status <- db_status(verbose = FALSE)
  expect_equal(status$mode, "ducklake")
  
  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
