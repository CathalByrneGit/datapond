# tests/testthat/test-db_read.R

# ==============================================================================
# Tests for db_read() - Connection Checks
# ==============================================================================

test_that("db_read errors when not connected", {
  clean_db_env()

  expect_error(
    db_read(table = "imports"),
    "Not connected"
  )
})

# ==============================================================================
# Tests for db_read() - Input Validation
# ==============================================================================

test_that("db_read validates schema name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_schema.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_read(schema = "", table = "t"), "must be a single, non-empty string")
  expect_error(db_read(schema = "../bad", table = "t"), "potentially dangerous characters")
  expect_error(db_read(schema = 123, table = "t"), "must be a single, non-empty string")

  clean_db_env()
})

test_that("db_read validates table name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_table.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_read(table = ""), "must be a single, non-empty string")
  expect_error(db_read(table = "bad;drop"), "potentially dangerous characters")
  expect_error(db_read(table = NULL), "must be a single, non-empty string")

  clean_db_env()
})

test_that("db_read rejects both version and timestamp", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_tt.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_read(table = "test", version = 1, timestamp = "2025-01-01"),
    "only one of"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_read() - Integration with DuckLake
# ==============================================================================

test_that("db_read works with DuckLake table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Setup
  temp_dir <- tempfile(pattern = "lake_read_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create a table
  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'Widget'), (2, 'Gadget')")

  # Read it back
  result <- db_read(schema = "main", table = "products")

  expect_s3_class(result, "tbl_lazy")

  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 2)
  expect_true("id" %in% names(collected))
  expect_true("name" %in% names(collected))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_read time travel by version works", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Setup
  temp_dir <- tempfile(pattern = "lake_tt_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create table (version 1)
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER, status VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1, 'active')")

  # Get version 1 snapshot
  v1 <- DBI::dbGetQuery(con, "SELECT MAX(snapshot_id) as v FROM ducklake_snapshots('test')")$v

  # Modify (version 2)
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (2, 'pending')")

  # Current data should have 2 rows
  current <- db_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(current), 2)

  # Version 1 should have 1 row
  v1_data <- db_read(table = "items", version = v1) |> dplyr::collect()
  expect_equal(nrow(v1_data), 1)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_read errors for non-existent table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_err_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_read(table = "nonexistent"),
    "Unable to read table"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_read works with different schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_schema_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create a custom schema and table
  DBI::dbExecute(con, "CREATE SCHEMA test.trade")
  DBI::dbExecute(con, "CREATE TABLE test.trade.imports (id INTEGER, value DOUBLE)")
  DBI::dbExecute(con, "INSERT INTO test.trade.imports VALUES (1, 100.5), (2, 200.75)")

  # Read from custom schema
  result <- db_read(schema = "trade", table = "imports")
  collected <- dplyr::collect(result)

  expect_equal(nrow(collected), 2)
  expect_equal(sum(collected$value), 301.25)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
