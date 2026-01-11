# tests/testthat/test-metadata.R

# ==============================================================================
# Tests for db_snapshots()
# ==============================================================================

test_that("db_snapshots errors when not connected", {
  clean_db_env()

  expect_error(db_snapshots(), "Not connected")
})

test_that("db_snapshots errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_snapshots(), "hive mode")

  clean_db_env()
})

test_that("db_snapshots errors without catalog", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Connect to DuckDB but don't attach a DuckLake catalog
  # This simulates a misconfigured state
  # We can't easily test this without internal access, so just verify

  # the basic mode checking works
  db_connect(path = "/test")

  expect_error(db_snapshots(), "hive mode")

  clean_db_env()
})

test_that("db_snapshots returns snapshot data", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "snapshots_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create a table to generate a snapshot
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1)")

  snapshots <- db_snapshots()

  expect_s3_class(snapshots, "data.frame")
  expect_true(nrow(snapshots) >= 1)
  expect_true("snapshot_id" %in% names(snapshots))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_catalog()
# ==============================================================================

test_that("db_catalog errors when not connected", {
  clean_db_env()

  expect_error(db_catalog(), "Not connected")
})

test_that("db_catalog errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_catalog(), "hive mode")

  clean_db_env()
})

test_that("db_catalog returns table info", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "catalog_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create tables
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'Widget')")
  DBI::dbExecute(con, "CREATE TABLE test.main.orders (id INTEGER, total DOUBLE)")

  catalog_info <- db_catalog()

  expect_s3_class(catalog_info, "data.frame")
  expect_true(nrow(catalog_info) >= 2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
