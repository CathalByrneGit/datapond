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

# ==============================================================================
# Tests for db_write() - col_types parameter
# ==============================================================================

test_that("db_write validates col_types parameter", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_coltypes_val_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, value = c(10.5, 20.5, 30.5))

  # col_types must be named
  expect_error(
    db_write(df, table = "test", col_types = c("BIGINT", "DOUBLE")),
    "must have names"
  )

  # col_types columns must exist in data
  expect_error(
    db_write(df, table = "test", col_types = list(nonexistent = "BIGINT")),
    "not found in data"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write applies col_types correctly", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_coltypes_apply_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, value = c(10.5, 20.5, 30.5))

  # Write with explicit column types
  db_write(df, table = "test_table", col_types = list(id = "BIGINT", value = "DECIMAL(10,2)"))

  # Verify table was created and data is readable
  result <- db_read(table = "test_table") |> dplyr::collect()
  expect_equal(nrow(result), 3)

  # Check column types via SQL
  con <- datapond:::.db_get_con()
  schema_info <- DBI::dbGetQuery(con, "DESCRIBE test.main.test_table")

  # id should be BIGINT
  expect_true(grepl("BIGINT", schema_info$column_type[schema_info$column_name == "id"], ignore.case = TRUE))
  # value should be DECIMAL
  expect_true(grepl("DECIMAL", schema_info$column_type[schema_info$column_name == "value"], ignore.case = TRUE))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write accepts col_types as character vector", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_coltypes_char_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, name = c("a", "b", "c"))

  # Use character vector shorthand
  db_write(df, table = "test_table", col_types = c(id = "INTEGER"))

  result <- db_read(table = "test_table") |> dplyr::collect()
  expect_equal(nrow(result), 3)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_write() - bucket_by parameter
# ==============================================================================

test_that("db_write validates bucket_by parameter", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_bucket_val_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(user_id = 1:5, value = c(10, 20, 30, 40, 50))

  # bucket_by must be a list with column and buckets
  expect_error(
    db_write(df, table = "test", bucket_by = "user_id"),
    "must be a list"
  )

  expect_error(
    db_write(df, table = "test", bucket_by = list(column = "user_id")),
    "must be a list with 'column' and 'buckets'"
  )

  # bucket_by column must exist
  expect_error(
    db_write(df, table = "test", bucket_by = list(column = "nonexistent", buckets = 8)),
    "not found in data"
  )

  # buckets must be positive
  expect_error(
    db_write(df, table = "test", bucket_by = list(column = "user_id", buckets = 0)),
    "positive integer"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write bucket_by cannot be used with append mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_bucket_append_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(user_id = 1:3, value = c(10, 20, 30))

  # Create table first
  db_write(df, table = "test", mode = "overwrite")

  # bucket_by not allowed with append
  expect_error(
    db_write(df, table = "test", mode = "append", bucket_by = list(column = "user_id", buckets = 8)),
    "cannot be used with mode = 'append'"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write creates bucket-partitioned table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_bucket_create_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(
    user_id = 1:10,
    event = paste0("event_", 1:10),
    value = runif(10)
  )

  # Create with bucket partitioning
  db_write(df, table = "events", bucket_by = list(column = "user_id", buckets = 4))

  # Verify data is readable
  result <- db_read(table = "events") |> dplyr::collect()
  expect_equal(nrow(result), 10)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_write() - sort_by parameter
# ==============================================================================

test_that("db_write validates sort_by parameter", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_sortby_val_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:5, date = as.Date("2024-01-01") + 0:4, value = runif(5))

  # sort_by must be non-empty character vector
  expect_error(
    db_write(df, table = "test", sort_by = character(0)),
    "must be a non-empty character vector"
  )

  # sort_by columns must exist
  expect_error(
    db_write(df, table = "test", sort_by = "nonexistent"),
    "not found in data"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write sort_by cannot be used with append mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_sortby_append_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, date = as.Date("2024-01-01") + 0:2, value = runif(3))

  # Create table first
  db_write(df, table = "test", mode = "overwrite")

  # sort_by not allowed with append
  expect_error(
    db_write(df, table = "test", mode = "append", sort_by = "date"),
    "cannot be used with mode = 'append'"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write creates sorted/clustered table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_sortby_create_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(
    id = 1:10,
    sale_date = as.Date("2024-01-01") + 0:9,
    region = rep(c("North", "South"), 5),
    value = runif(10)
  )

  # Create with clustering
  db_write(df, table = "sales", sort_by = c("sale_date", "region"))

  # Verify data is readable
  result <- db_read(table = "sales") |> dplyr::collect()
  expect_equal(nrow(result), 10)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_write() - inline parameter (DEPRECATED)
# ==============================================================================

# Note: The inline parameter is deprecated. DuckLake automatically inlines
# small writes based on data_inlining_row_limit threshold.

test_that("db_write inline parameter shows deprecation message", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_inline_append_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create initial table
  df1 <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_write(df1, table = "events", mode = "overwrite")

  # Append with inline=TRUE should show deprecation message
  df2 <- data.frame(id = 4:6, value = c(40, 50, 60))
  expect_message(
    db_write(df2, table = "events", mode = "append", inline = TRUE),
    "deprecated"
  )

  # Verify data is still written correctly
  result <- db_read(table = "events") |> dplyr::collect()
  expect_equal(nrow(result), 6)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_write() - Combined parameters
# ==============================================================================

test_that("db_write combines hive partitioning and bucket partitioning", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_combined_part_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(
    user_id = rep(1:5, each = 2),
    year = as.integer(rep(c(2023, 2024), 5)),
    value = runif(10)
  )

  # Create with combined partitioning
  db_write(df, table = "events",
           partition_by = "year",
           bucket_by = list(column = "user_id", buckets = 4))

  # Verify data is readable
  result <- db_read(table = "events") |> dplyr::collect()
  expect_equal(nrow(result), 10)

  # Verify hive partitioning is set
  parts <- db_get_partitioning(table = "events")
  expect_true("year" %in% parts)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_write() - Lazy table (zero-copy) support
# ==============================================================================

test_that("db_write accepts lazy dbplyr table for overwrite", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_lazy_overwrite_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create source table
  source_data <- data.frame(
    id = 1:10,
    value = c(10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
    category = rep(c("A", "B"), 5)
  )
  db_write(source_data, table = "source")

  # Read as lazy table, transform, and write - no collect()
  db_read(table = "source") |>
    dplyr::filter(value > 30) |>
    dplyr::mutate(value_doubled = value * 2) |>
    db_write(table = "transformed")

  # Verify the result
  result <- db_read(table = "transformed") |> dplyr::collect()
  expect_equal(nrow(result), 7)  # values > 30
  expect_true("value_doubled" %in% names(result))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write accepts lazy dbplyr table for append", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_lazy_append_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create source and target tables
  source_data <- data.frame(id = 1:5, value = c(10, 20, 30, 40, 50))
  target_data <- data.frame(id = 100:102, value = c(100, 200, 300))

  db_write(source_data, table = "source")
  db_write(target_data, table = "target")

  # Append from lazy query
  db_read(table = "source") |>
    dplyr::filter(value >= 30) |>
    db_write(table = "target", mode = "append")

  # Verify
  result <- db_read(table = "target") |> dplyr::collect()
  expect_equal(nrow(result), 6)  # 3 original + 3 appended

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write with lazy table supports aggregations", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_lazy_agg_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create source table
  source_data <- data.frame(
    category = rep(c("A", "B", "C"), each = 4),
    value = 1:12
  )
  db_write(source_data, table = "source")

  # Aggregate and write
  db_read(table = "source") |>
    dplyr::group_by(category) |>
    dplyr::summarise(total = sum(value, na.rm = TRUE), .groups = "drop") |>
    db_write(table = "summary")

  # Verify
  result <- db_read(table = "summary") |> dplyr::collect()
  expect_equal(nrow(result), 3)
  expect_true("total" %in% names(result))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_write with lazy table warns about col_types", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_lazy_coltypes_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create source table
  db_write(data.frame(id = 1:3, value = c(1.1, 2.2, 3.3)), table = "source")

  # Should warn that col_types is ignored for lazy tables
  expect_warning(
    db_read(table = "source") |>
      db_write(table = "dest", col_types = list(id = "BIGINT")),
    "col_types is ignored"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
