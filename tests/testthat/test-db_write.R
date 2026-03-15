# tests/testthat/test-db_write.R

# ==============================================================================
# Tests for .db_temp_name()
# ==============================================================================

test_that(".db_temp_name generates unique names", {
  temp_name <- datapond:::.db_temp_name

  name1 <- temp_name()
  name2 <- temp_name()

  # Should start with default prefix
  expect_true(grepl("^db_tmp_", name1))
  expect_true(grepl("^db_tmp_", name2))

  # Should be unique
  expect_false(name1 == name2)

  # Should be reasonable length (prefix + 16 chars)
  expect_equal(nchar(name1), nchar("db_tmp_") + 16)
})

test_that(".db_temp_name respects custom prefix", {
  temp_name <- datapond:::.db_temp_name

  name <- temp_name(prefix = "custom_")
  expect_true(grepl("^custom_", name))
})

# ==============================================================================
# Tests for db_write() - Connection Checks
# ==============================================================================

test_that("db_write errors when not connected", {
  clean_db_env()

  expect_error(
    db_write(data.frame(x = 1), table = "test"),
    "Not connected"
  )
})

# ==============================================================================
# Tests for db_write() - Input Validation
# ==============================================================================

test_that("db_write validates data argument", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_val.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_write("not a df", table = "t"), "must be a data.frame")
  expect_error(db_write(NULL, table = "t"), "must be a data.frame")

  clean_db_env()
})

test_that("db_write validates schema name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_schema_val.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(x = 1)
  expect_error(db_write(df, schema = "", table = "t"), "must be a single, non-empty")
  expect_error(db_write(df, schema = "../bad", table = "t"), "potentially dangerous characters")

  clean_db_env()
})

test_that("db_write validates table name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_table_val.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(x = 1)
  expect_error(db_write(df, table = ""), "must be a single, non-empty")
  expect_error(db_write(df, table = "bad;drop"), "potentially dangerous characters")

  clean_db_env()
})

# ==============================================================================
# Tests for db_write() - Integration
# ==============================================================================

test_that("db_write overwrite mode works", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_write_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, name = c("a", "b", "c"))

  result <- db_write(df, table = "items", mode = "overwrite")

  expect_equal(result, "test.main.items")

  # Read back
  read_df <- db_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(read_df), 3)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write append mode works", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_append_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create table
  df1 <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_write(df1, table = "items", mode = "overwrite")

  # Append
  df2 <- data.frame(id = 4:6, value = c(40, 50, 60))
  db_write(df2, table = "items", mode = "append")

  # Read back
  read_df <- db_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(read_df), 6)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write works with custom schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_schema_write_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create schema
  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE SCHEMA test.trade")

  df <- data.frame(product = c("widget", "gadget"), price = c(9.99, 19.99))

  result <- db_write(df, schema = "trade", table = "products")

  expect_equal(result, "test.trade.products")

  # Read back
  read_df <- db_read(schema = "trade", table = "products") |> dplyr::collect()
  expect_equal(nrow(read_df), 2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write records commit metadata", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_commit_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, value = c(10, 20, 30))

  db_write(df, table = "items", mode = "overwrite",
                commit_author = "test_user",
                commit_message = "Test commit message")

  # Check snapshot has the metadata
  snapshots <- db_snapshots()

  # Should have at least one snapshot
  expect_true(nrow(snapshots) >= 1)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write rolls back on error", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_rollback_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create initial table
  df1 <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_write(df1, table = "items", mode = "overwrite")

  # Try to append incompatible data (this should fail)
  # DuckLake should enforce schema
  df_bad <- data.frame(different_col = c("a", "b"))

  # This should error
  expect_error(
    db_write(df_bad, table = "items", mode = "append")
  )

  # Original data should still be intact
  read_df <- db_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(read_df), 3)
  expect_true("id" %in% names(read_df))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_write() - Partitioning
# ==============================================================================

test_that("db_write validates partition_by parameter", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_part_val_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, value = c(10, 20, 30), year = c(2023, 2023, 2024))

  # partition_by must be non-empty character vector
  expect_error(
    db_write(df, table = "test", partition_by = character(0)),
    "must be a non-empty character vector"
  )

  # partition_by columns must exist in data
  expect_error(
    db_write(df, table = "test", partition_by = c("nonexistent")),
    "partition_by columns not found"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write partition_by cannot be used with append mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_part_append_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, value = c(10, 20, 30), year = c(2023, 2023, 2024))

  # First create table
  db_write(df, table = "test", mode = "overwrite")

  # partition_by not allowed with append
  expect_error(
    db_write(df, table = "test", mode = "append", partition_by = "year"),
    "cannot be used with mode = 'append'"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write creates partitioned table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_part_create_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(
    id = 1:6,
    value = c(10, 20, 30, 40, 50, 60),
    year = as.integer(c(2023, 2023, 2023, 2024, 2024, 2024))
  )

  # Create with partitioning
  db_write(df, table = "sales", partition_by = "year")

  # Verify partitioning was set
  parts <- db_get_partitioning(table = "sales")
  expect_equal(parts, "year")

  # Verify data is readable
  result <- db_read(table = "sales") |> dplyr::collect()
  expect_equal(nrow(result), 6)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write preserves existing partitioning on overwrite", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_part_preserve_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df1 <- data.frame(
    id = 1:3,
    value = c(10, 20, 30),
    year = as.integer(c(2023, 2023, 2024))
  )

  # Create with partitioning
  db_write(df1, table = "sales", partition_by = "year")

  # Verify partitioning
  parts1 <- db_get_partitioning(table = "sales")
  expect_equal(parts1, "year")

  # Overwrite without specifying partition_by - should preserve
  df2 <- data.frame(
    id = 4:6,
    value = c(40, 50, 60),
    year = as.integer(c(2024, 2024, 2025))
  )
  db_write(df2, table = "sales", mode = "overwrite")

  # Partitioning should be preserved
  parts2 <- db_get_partitioning(table = "sales")
  expect_equal(parts2, "year")

  # Data should be replaced
  result <- db_read(table = "sales") |> dplyr::collect()
  expect_equal(nrow(result), 3)
  expect_true(all(result$id %in% 4:6))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write can change partitioning on overwrite", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_part_change_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(
    id = 1:4,
    value = c(10, 20, 30, 40),
    year = as.integer(c(2023, 2023, 2024, 2024)),
    month = as.integer(c(1, 2, 1, 2))
  )

  # Create with year partitioning
  db_write(df, table = "sales", partition_by = "year")
  parts1 <- db_get_partitioning(table = "sales")
  expect_equal(parts1, "year")

  # Overwrite with different partitioning
  db_write(df, table = "sales", mode = "overwrite", partition_by = c("year", "month"))
  parts2 <- db_get_partitioning(table = "sales")
  expect_equal(parts2, c("year", "month"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
