# tests/testthat/test-maintenance.R

# ==============================================================================
# Tests for db_vacuum()
# ==============================================================================

test_that("db_vacuum errors when not connected", {
  clean_db_env()

  expect_error(db_vacuum(), "Not connected")
})

test_that("db_vacuum errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_vacuum(), "hive mode")

  clean_db_env()
})

test_that("db_vacuum errors without catalog", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Connect in hive mode - simulates no catalog configured
  db_connect(path = "/test")

  expect_error(db_vacuum(), "hive mode")

  clean_db_env()
})

test_that("db_vacuum validates older_than argument", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_vacuum.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_vacuum(older_than = 123), "POSIXct, difftime, or character")
  expect_error(db_vacuum(older_than = list()), "POSIXct, difftime, or character")

  clean_db_env()
})

test_that("db_vacuum dry_run reports without deleting", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "vacuum_dry_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create some data to generate snapshots
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1)")

  snapshots_before <- db_snapshots()

  # Dry run with a reasonable interval - should not delete anything
  expect_output(
    result <- db_vacuum(older_than = "30 days", dry_run = TRUE),
    "DRY RUN"
  )

  snapshots_after <- db_snapshots()
  expect_equal(nrow(snapshots_before), nrow(snapshots_after))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_vacuum accepts POSIXct timestamp", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "vacuum_posix_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER)")

  # Should not error with POSIXct
  cutoff <- Sys.time() - 86400  # 1 day ago
  expect_output(
    db_vacuum(older_than = cutoff, dry_run = TRUE),
    "DRY RUN"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_vacuum accepts difftime", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "vacuum_diff_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER)")

  # Should not error with difftime
  expect_output(
    db_vacuum(older_than = as.difftime(7, units = "days"), dry_run = TRUE),
    "DRY RUN"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_rollback()
# ==============================================================================

test_that("db_rollback errors when not connected", {
  clean_db_env()

  expect_error(db_rollback(table = "test", version = 1), "Not connected")
})

test_that("db_rollback errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_rollback(table = "test", version = 1), "hive mode")

  clean_db_env()
})

test_that("db_rollback validates name arguments", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_rb.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_rollback(table = "", version = 1), "non-empty")
  expect_error(db_rollback(schema = "", table = "test", version = 1), "non-empty")

  clean_db_env()
})

test_that("db_rollback requires version or timestamp", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_rb2.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_rollback(table = "test"), "Must specify either")

  clean_db_env()
})

test_that("db_rollback rejects both version and timestamp", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_rb3.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_rollback(table = "test", version = 1, timestamp = "2025-01-01"),
    "only one of"
  )

  clean_db_env()
})

test_that("db_rollback by version works", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "rollback_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create table with initial data
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'Original')")

  # Get the initial snapshot version
  snapshots <- db_snapshots()
  initial_version <- max(snapshots$snapshot_id)

  # Modify the data
  DBI::dbExecute(con, "UPDATE test.main.products SET name = 'Modified' WHERE id = 1")

  # Verify modification
  current <- db_lake_read(table = "products") |> dplyr::collect()
  expect_equal(current$name, "Modified")

  # Rollback to initial version
  expect_message(
    db_rollback(table = "products", version = initial_version),
    "Rolled back"
  )

  # Verify rollback
  after_rollback <- db_lake_read(table = "products") |> dplyr::collect()
  expect_equal(after_rollback$name, "Original")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_rollback returns qualified table name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "rollback_return_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "mycat",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE mycat.main.items (id INTEGER)")
  DBI::dbExecute(con, "INSERT INTO mycat.main.items VALUES (1)")

  snapshots <- db_snapshots()
  version <- max(snapshots$snapshot_id)

  result <- db_rollback(table = "items", version = version)
  expect_equal(result, "mycat.main.items")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_diff()
# ==============================================================================

test_that("db_diff errors when not connected", {
  clean_db_env()

  expect_error(db_diff(table = "test", from_version = 1), "Not connected")
})

test_that("db_diff errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_diff(table = "test", from_version = 1), "hive mode")

  clean_db_env()
})

test_that("db_diff validates name arguments", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_diff.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_diff(table = "", from_version = 1), "non-empty")

  clean_db_env()
})

test_that("db_diff requires from reference", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_diff2.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_diff(table = "test"), "Must specify either")

  clean_db_env()
})

test_that("db_diff validates key_cols", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "diff_keycols_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1)")

  snapshots <- db_snapshots()
  version <- max(snapshots$snapshot_id)

  expect_error(
    db_diff(table = "items", from_version = version, key_cols = 123),
    "character vector"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_diff returns added and removed rows", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "diff_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create initial data
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1, 'One'), (2, 'Two')")

  snapshots <- db_snapshots()
  v1 <- max(snapshots$snapshot_id)

  # Modify data - add one, remove one
  DBI::dbExecute(con, "DELETE FROM test.main.items WHERE id = 2")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (3, 'Three')")

  # Get diff
  diff <- db_diff(table = "items", from_version = v1)

  expect_true(is.list(diff))
  expect_true("added" %in% names(diff))
  expect_true("removed" %in% names(diff))

  expect_equal(nrow(diff$added), 1)
  expect_equal(diff$added$id, 3)

  expect_equal(nrow(diff$removed), 1)
  expect_equal(diff$removed$id, 2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_diff with key_cols returns modified rows", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "diff_modified_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create initial data
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1, 'One'), (2, 'Two')")

  snapshots <- db_snapshots()
  v1 <- max(snapshots$snapshot_id)

  # Modify existing row
  DBI::dbExecute(con, "UPDATE test.main.items SET name = 'Two Updated' WHERE id = 2")

  # Get diff with key_cols
  diff <- db_diff(table = "items", from_version = v1, key_cols = "id")

  expect_true("modified" %in% names(diff))
  expect_equal(nrow(diff$modified), 1)
  expect_equal(diff$modified$id, 2)
  expect_equal(diff$modified$name, "Two Updated")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_diff collect=FALSE returns lazy tbls", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "diff_lazy_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1)")

  snapshots <- db_snapshots()
  v1 <- max(snapshots$snapshot_id)

  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (2)")

  # Get lazy diff (no summary output)
  diff <- db_diff(table = "items", from_version = v1, collect = FALSE)

  # Should be lazy tbls, not data.frames
  expect_true(inherits(diff$added, "tbl_lazy") || inherits(diff$added, "tbl_sql"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_query()
# ==============================================================================

test_that("db_query errors when not connected", {
  clean_db_env()

  expect_error(db_query("SELECT 1"), "Not connected")
})

test_that("db_query validates sql argument", {
  clean_db_env()
  db_connect(path = tempdir())

  expect_error(db_query(""), "non-empty string")
  expect_error(db_query(123), "non-empty string")
  expect_error(db_query(c("SELECT 1", "SELECT 2")), "non-empty string")
  expect_error(db_query(NULL), "non-empty string")

  clean_db_env()
})

test_that("db_query returns collected data.frame by default", {
  clean_db_env()
  db_connect(path = tempdir())

  result <- db_query("SELECT 1 AS value, 'test' AS name")

  expect_s3_class(result, "data.frame")
  expect_equal(result$value, 1)
  expect_equal(result$name, "test")

  clean_db_env()
})

test_that("db_query with collect=FALSE returns lazy tbl", {
  clean_db_env()
  db_connect(path = tempdir())

  result <- db_query("SELECT 1 AS value", collect = FALSE)

  expect_true(inherits(result, "tbl_lazy") || inherits(result, "tbl_sql"))

  # Can still collect it
  collected <- dplyr::collect(result)
  expect_equal(collected$value, 1)

  clean_db_env()
})

test_that("db_query works in hive mode", {
  clean_db_env()
  db_connect(path = tempdir())

  # Simple query should work
  result <- db_query("SELECT 42 AS answer")
  expect_equal(result$answer, 42)

  clean_db_env()
})

test_that("db_query works in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "query_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.items VALUES (1, 'One'), (2, 'Two')")

  result <- db_query("SELECT * FROM test.main.items ORDER BY id")

  expect_equal(nrow(result), 2)
  expect_equal(result$name, c("One", "Two"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_query propagates SQL errors", {
  clean_db_env()
  db_connect(path = tempdir())

  expect_error(db_query("SELECT * FROM nonexistent_table_xyz"))

  clean_db_env()
})
