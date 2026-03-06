# tests/testthat/test-db_connect.R

# ==============================================================================
# Test Setup and Helpers
# ==============================================================================

# Helper to clean up connection state between tests
clean_db_env <- function() {
 # Access the package's internal environment
  env <- datapond:::.db_env

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
 validate <- datapond:::.db_validate_name

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
  validate <- datapond:::.db_validate_name

  # Empty/NULL/NA
  expect_error(validate(""), "must be a single, non-empty string")
  expect_error(validate(NA_character_), "must be a single, non-empty string")
  expect_error(validate(NULL), "must be a single, non-empty string")

  # Wrong types
  expect_error(validate(123), "must be a single, non-empty string")
  expect_error(validate(TRUE), "must be a single, non-empty string")
  expect_error(validate(c("a", "b")), "must be a single, non-empty string")

  # Path traversal attempts - blocked as dangerous
  expect_error(validate("../etc"), "potentially dangerous characters")
  expect_error(validate("Trade/Imports"), "contains invalid characters")  # / not in dangerous pattern
  expect_error(validate("Trade\\Imports"), "potentially dangerous characters")
  expect_error(validate(".."), "potentially dangerous characters")

  # SQL injection attempts - blocked as dangerous
  expect_error(validate("Trade; DROP TABLE"), "potentially dangerous characters")
  expect_error(validate("Trade'--"), "potentially dangerous characters")
  expect_error(validate("Trade\""), "potentially dangerous characters")

  # Special characters - blocked as invalid (not dangerous)
  expect_error(validate("Trade Imports"), "contains invalid characters")  # space
  expect_error(validate("Trade.Imports"), "contains invalid characters")  # dot
  expect_error(validate("Trade@Imports"), "contains invalid characters")  # @
  expect_error(validate("Trade#1"), "contains invalid characters")        # #
})

test_that(".db_validate_name uses custom arg name in errors", {
  validate <- datapond:::.db_validate_name

  expect_error(validate("", arg = "schema"), "schema must be a single")
  expect_error(validate("bad/name", arg = "table"), "table contains invalid")
})

# ==============================================================================
# Tests for .db_sql_quote()
# ==============================================================================

test_that(".db_sql_quote properly quotes strings", {
  sql_quote <- datapond:::.db_sql_quote

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
  env <- datapond:::.db_env
  db_get <- datapond:::.db_get

  # Set up test values
  assign("test_value", "hello", envir = env)
  assign("test_number", 42, envir = env)

  expect_equal(db_get("test_value"), "hello")
  expect_equal(db_get("test_number"), 42)

  # Cleanup
  rm("test_value", "test_number", envir = env)
})

test_that(".db_get returns default when not exists", {
  db_get <- datapond:::.db_get

  expect_null(db_get("nonexistent"))
  expect_equal(db_get("nonexistent", default = "default_val"), "default_val")
  expect_equal(db_get("nonexistent", default = NA_character_), NA_character_)
})

# ==============================================================================
# Tests for .db_build_ducklake_dsn()
# ==============================================================================

test_that(".db_build_ducklake_dsn builds correct DSN for each catalog type", {
  build_dsn <- datapond:::.db_build_ducklake_dsn

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
  build_dsn <- datapond:::.db_build_ducklake_dsn

  expect_error(build_dsn("mysql", "catalog.db"), "Unknown catalog_type")
  expect_error(build_dsn("oracle", "catalog.db"), "Unknown catalog_type")
  expect_error(build_dsn("", "catalog.db"), "Unknown catalog_type")
})

# ==============================================================================
# Tests for db_disconnect()
# ==============================================================================

test_that("db_disconnect returns FALSE when not connected", {
  clean_db_env()

  result <- db_disconnect()
  expect_false(result)
})

# ==============================================================================
# Tests for db_status()
# ==============================================================================

test_that("db_status returns correct status when disconnected", {
  clean_db_env()

  status <- db_status(verbose = FALSE)

  expect_false(status$connected)
  expect_true(is.na(status$data_path))
})

# ==============================================================================
# Tests for db_connect() - DuckLake Mode
# ==============================================================================

test_that("db_connect validates catalog_type", {
  clean_db_env()

  # Invalid catalog type should error (match.arg)
  expect_error(
    db_connect(catalog_type = "mysql"),
    "'arg' should be one of"
  )

  expect_error(
    db_connect(catalog_type = "invalid"),
    "'arg' should be one of"
  )
})

test_that("db_connect validates catalog name", {
  clean_db_env()

  expect_error(
    db_connect(catalog = "../bad"),
    "potentially dangerous characters"
  )

  expect_error(
    db_connect(catalog = "cat; DROP"),
    "potentially dangerous characters"
  )
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

test_that("db_connect works with DuckDB catalog", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Create temp directory for test
  temp_dir <- tempdir()
  metadata_file <- file.path(temp_dir, "test_catalog.ducklake")
  data_dir <- file.path(temp_dir, "test_data")
  dir.create(data_dir, showWarnings = FALSE)

  con <- db_connect(
    catalog = "test",
    catalog_type = "duckdb",
    metadata_path = metadata_file,
    data_path = data_dir
  )

  expect_true(DBI::dbIsValid(con))

  status <- db_status(verbose = FALSE)
  expect_equal(status$catalog, "test")
  expect_equal(status$catalog_type, "duckdb")

  clean_db_env()

  # Cleanup
  unlink(metadata_file)
  unlink(data_dir, recursive = TRUE)
})

test_that("db_connect stores correct state for SQLite catalog", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Create temp directory for test
  temp_dir <- tempdir()
  metadata_file <- file.path(temp_dir, "test_catalog.sqlite")
  data_dir <- file.path(temp_dir, "test_data_sqlite")
  dir.create(data_dir, showWarnings = FALSE)

  con <- db_connect(
    catalog = "test_sqlite",
    catalog_type = "sqlite",
    metadata_path = metadata_file,
    data_path = data_dir
  )

  expect_true(DBI::dbIsValid(con))

  status <- db_status(verbose = FALSE)
  expect_equal(status$catalog, "test_sqlite")
  expect_equal(status$catalog_type, "sqlite")

  clean_db_env()

  # Cleanup
  unlink(metadata_file)
  unlink(data_dir, recursive = TRUE)
})

test_that("db_connect returns existing connection for same lake", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "reuse_test_")
  dir.create(temp_dir)

  metadata_file <- file.path(temp_dir, "catalog.ducklake")
  data_dir <- temp_dir

  # First connection
  con1 <- db_connect(
    metadata_path = metadata_file,
    data_path = data_dir
  )

  # Second connection to same lake - should reuse
  con2 <- db_connect(
    metadata_path = metadata_file,
    data_path = data_dir
  )

  expect_identical(con1, con2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_connect disconnects when connecting to different lake", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir1 <- tempfile(pattern = "lake1_")
  temp_dir2 <- tempfile(pattern = "lake2_")
  dir.create(temp_dir1)
  dir.create(temp_dir2)

  # First connection
  con1 <- db_connect(
    metadata_path = file.path(temp_dir1, "catalog.ducklake"),
    data_path = temp_dir1
  )

  # Second connection to different lake - should disconnect and reconnect
  expect_message(
    con2 <- db_connect(
      metadata_path = file.path(temp_dir2, "catalog.ducklake"),
      data_path = temp_dir2
    ),
    "Disconnecting"
  )

  expect_false(identical(con1, con2))

  clean_db_env()
  unlink(temp_dir1, recursive = TRUE)
  unlink(temp_dir2, recursive = TRUE)
})

# ==============================================================================
# Tests for .db_get_con()
# ==============================================================================

test_that(".db_get_con returns NULL when not connected", {
  clean_db_env()

  result <- datapond:::.db_get_con()
  expect_null(result)
})

test_that(".db_get_con returns NULL for invalid connection", {
  clean_db_env()
  env <- datapond:::.db_env

  # Create a connection, then invalidate it manually
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  assign("con", con, envir = env)
  DBI::dbDisconnect(con, shutdown = TRUE)

  # .db_get_con should recognize it's invalid
  result <- datapond:::.db_get_con()
  expect_null(result)

  clean_db_env()
})
