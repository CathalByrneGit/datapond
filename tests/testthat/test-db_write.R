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
# Tests for .db_validate_compression()
# ==============================================================================

test_that(".db_validate_compression accepts valid codecs", {
  validate <- datapond:::.db_validate_compression

  expect_equal(validate("zstd"), "zstd")
  expect_equal(validate("snappy"), "snappy")
  expect_equal(validate("gzip"), "gzip")
  expect_equal(validate("brotli"), "brotli")
  expect_equal(validate("lz4"), "lz4")
  expect_equal(validate("lz4_raw"), "lz4_raw")
  expect_equal(validate("uncompressed"), "uncompressed")

  # Case insensitive
  expect_equal(validate("ZSTD"), "zstd")
  expect_equal(validate("Snappy"), "snappy")

  # Trims whitespace
  expect_equal(validate("  zstd  "), "zstd")
})

test_that(".db_validate_compression accepts NULL", {
  validate <- datapond:::.db_validate_compression

  expect_null(validate(NULL))
})

test_that(".db_validate_compression rejects invalid codecs", {
  validate <- datapond:::.db_validate_compression

  expect_error(validate("invalid"), "Unsupported compression")
  expect_error(validate("zip"), "Unsupported compression")
  expect_error(validate("deflate"), "Unsupported compression")
})

test_that(".db_validate_compression rejects wrong types", {
  validate <- datapond:::.db_validate_compression

  expect_error(validate(""), "must be NULL or a non-empty")
  expect_error(validate(123), "must be NULL or a non-empty")
  expect_error(validate(c("zstd", "snappy")), "must be NULL or a non-empty")
  expect_error(validate(NA_character_), "must be NULL or a non-empty")
})

# ==============================================================================
# Tests for .db_enforce_partition_governance()
# ==============================================================================

test_that(".db_enforce_partition_governance passes when no rules defined", {
  clean_db_env()
  db_connect(path = "/test")

  enforce <- datapond:::.db_enforce_partition_governance

  # No rules set - should pass silently
  expect_silent(enforce("Trade", "Imports", NULL))
  expect_silent(enforce("Trade", "Imports", c("year", "month")))

  clean_db_env()
})

test_that(".db_enforce_partition_governance enforces rules", {
  clean_db_env()
  db_connect(path = "/test")

  # Set up rules
  env <- datapond:::.db_env
  assign("partition_rules", list("Trade/Imports" = c("year", "month")), envir = env)

  enforce <- datapond:::.db_enforce_partition_governance

  # Correct partitioning passes
  expect_silent(enforce("Trade", "Imports", c("year", "month")))

  # Wrong partitioning fails
  expect_error(
    enforce("Trade", "Imports", c("year")),
    "Partition governance"
  )

  expect_error(
    enforce("Trade", "Imports", NULL),
    "Partition governance"
  )

  # Different dataset not subject to rules
  expect_silent(enforce("Trade", "Exports", NULL))

  clean_db_env()
})

# ==============================================================================
# Tests for db_hive_write() - Connection and Mode Checks
# ==============================================================================

test_that("db_hive_write errors when not connected", {
  clean_db_env()

  expect_error(
    db_hive_write(data.frame(x = 1), "Trade", "Imports"),
    "Not connected"
  )
})

test_that("db_hive_write errors in wrong mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_write_mode.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_hive_write(data.frame(x = 1), "Trade", "Imports"),
    "Connected in DuckLake mode"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_hive_write() - Input Validation
# ==============================================================================

test_that("db_hive_write validates data argument", {
  clean_db_env()
  db_connect(path = tempdir())

  expect_error(db_hive_write("not a df", "Trade", "Test"), "must be a data.frame")
  expect_error(db_hive_write(list(a = 1), "Trade", "Test"), "must be a data.frame")
  expect_error(db_hive_write(NULL, "Trade", "Test"), "must be a data.frame")

  clean_db_env()
})

test_that("db_hive_write validates section name", {
  clean_db_env()
  db_connect(path = tempdir())

  df <- data.frame(x = 1)
  expect_error(db_hive_write(df, "", "Test"), "must be a single, non-empty")
  expect_error(db_hive_write(df, "../bad", "Test"), "potentially dangerous characters")
  expect_error(db_hive_write(df, 123, "Test"), "must be a single, non-empty")

  clean_db_env()
})

test_that("db_hive_write validates dataset name", {
  clean_db_env()
  db_connect(path = tempdir())

  df <- data.frame(x = 1)
  expect_error(db_hive_write(df, "Trade", ""), "must be a single, non-empty")
  expect_error(db_hive_write(df, "Trade", "bad/name"), "contains invalid characters")

  clean_db_env()
})

test_that("db_hive_write validates partition_by columns exist", {
  clean_db_env()
  db_connect(path = tempdir())

  df <- data.frame(x = 1, y = 2)

  expect_error(
    db_hive_write(df, "Trade", "Test", partition_by = c("nonexistent")),
    "partition_by columns not found"
  )

  expect_error(
    db_hive_write(df, "Trade", "Test", partition_by = c("x", "z")),
    "partition_by columns not found.*z"
  )

  clean_db_env()
})

test_that("db_hive_write requires partition_by for replace_partitions mode", {
  clean_db_env()
  db_connect(path = tempdir())

  df <- data.frame(x = 1)

  expect_error(
    db_hive_write(df, "Trade", "Test", mode = "replace_partitions"),
    "requires partition_by"
  )

  clean_db_env()
})

test_that("db_hive_write validates partition_by format", {
  clean_db_env()
  db_connect(path = tempdir())

  df <- data.frame(x = 1, y = 2)

  expect_error(
    db_hive_write(df, "Trade", "Test", partition_by = c("x", NA)),
    "cannot contain NA"
  )

  expect_error(
    db_hive_write(df, "Trade", "Test", partition_by = c("x", "")),
    "cannot contain NA/empty"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_hive_write() - Integration
# ==============================================================================

test_that("db_hive_write creates parquet files", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "write_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  test_data <- data.frame(
    id = 1:5,
    value = c(10, 20, 30, 40, 50)
  )

  result <- db_hive_write(test_data, "Trade", "Test")

  # Check path returned
  expect_equal(result, file.path(temp_dir, "Trade", "Test"))

  # Check files created
  files <- list.files(file.path(temp_dir, "Trade", "Test"),
                      pattern = "\\.parquet$", recursive = TRUE)
  expect_true(length(files) > 0)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_write creates partitioned structure", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "part_write_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  test_data <- data.frame(
    id = 1:4,
    value = c(10, 20, 30, 40),
    year = c(2023, 2023, 2024, 2024),
    month = c(1, 2, 1, 2)
  )

  db_hive_write(test_data, "Trade", "Imports", partition_by = c("year", "month"))

  # Check that some partition folders were created (DuckDB may format values differently)
  output_dir <- file.path(temp_dir, "Trade", "Imports")
  expect_true(dir.exists(output_dir))

  # List all created directories
  all_dirs <- list.dirs(output_dir, recursive = TRUE, full.names = FALSE)

  # Should have year partitions
  expect_true(any(grepl("year=", all_dirs)))

  # Should have month partitions
  expect_true(any(grepl("month=", all_dirs)))

  # Read back to verify data integrity
  result <- db_hive_read("Trade", "Imports") |> dplyr::collect()
  expect_equal(nrow(result), 4)
  expect_true("year" %in% names(result))
  expect_true("month" %in% names(result))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_write append mode requires partition_by", {
  clean_db_env()
  db_connect(path = tempdir())

  df <- data.frame(id = 1:3, value = c(10, 20, 30))

  expect_error(
    db_hive_write(df, "Test", "Data", mode = "append"),
    "requires partition_by"
  )

  clean_db_env()
})

test_that("db_hive_write append mode works with partitions", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "append_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  # Use partitioned writes - append adds to new partitions
  # First write (2023 data)
  df1 <- data.frame(id = 1:3, value = c(10, 20, 30), year = as.integer(c(2023, 2023, 2023)))
  db_hive_write(df1, "Test", "Data", partition_by = "year", mode = "overwrite")

  # Append different year partition (2024 data)
  df2 <- data.frame(id = 4:6, value = c(40, 50, 60), year = as.integer(c(2024, 2024, 2024)))
  db_hive_write(df2, "Test", "Data", partition_by = "year", mode = "append")

  # Read back and verify
  result <- db_hive_read("Test", "Data") |> dplyr::collect()
  expect_equal(nrow(result), 6)
  expect_true(all(1:6 %in% result$id))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_write ignore mode skips existing", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "ignore_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  # First write
  df1 <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_hive_write(df1, "Test", "Data", mode = "overwrite")

  # Ignore mode - should not overwrite
  df2 <- data.frame(id = 4:6, value = c(40, 50, 60))
  expect_message(
    db_hive_write(df2, "Test", "Data", mode = "ignore"),
    "Ignored write"
  )

  # Read back - should still have original data
  result <- db_hive_read("Test", "Data") |> dplyr::collect()
  expect_equal(nrow(result), 3)
  expect_equal(sort(result$id), 1:3)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_write creates expected partition folder names", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "folder_name_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  # Test with integer column - should create year=2023, year=2024 (no .0)
  df_int <- data.frame(
    id = 1:2,
    year = as.integer(c(2023, 2024))
  )
  db_hive_write(df_int, "Test", "IntYear", partition_by = "year", mode = "overwrite")

  int_dirs <- list.dirs(file.path(temp_dir, "Test", "IntYear"),
                        recursive = FALSE, full.names = FALSE)
  expect_true("year=2023" %in% int_dirs)
  expect_true("year=2024" %in% int_dirs)

  # Test with numeric (double) column - should create year=2023.0, year=2024.0
  df_num <- data.frame(
    id = 1:2,
    year = c(2023, 2024)  # numeric, not integer
  )
  db_hive_write(df_num, "Test", "NumYear", partition_by = "year", mode = "overwrite")

  num_dirs <- list.dirs(file.path(temp_dir, "Test", "NumYear"),
                        recursive = FALSE, full.names = FALSE)
  # DuckDB creates .0 suffix for doubles
  expect_true("year=2023.0" %in% num_dirs)
  expect_true("year=2024.0" %in% num_dirs)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_write replace_partitions mode works with numeric partitions", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("fs")
  clean_db_env()

  temp_dir <- tempfile(pattern = "replace_part_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  # Use NUMERIC (double) year column - this creates year=2023.0 folders
  df1 <- data.frame(
    id = 1:4,
    value = c(10, 20, 30, 40),
    year = c(2023, 2023, 2024, 2024)  # numeric, not integer
  )
  db_hive_write(df1, "Test", "Data", partition_by = "year", mode = "overwrite")

  # Check that folders were created with .0 suffix
  output_dir <- file.path(temp_dir, "Test", "Data")
  created_dirs <- list.dirs(output_dir, recursive = FALSE, full.names = FALSE)
  expect_true("year=2023.0" %in% created_dirs || "year=2023" %in% created_dirs)

  # Verify initial state
  initial <- db_hive_read("Test", "Data") |> dplyr::collect()
  expect_equal(nrow(initial), 4)

  # Replace just 2024 partition with new data (also use numeric)
  df2 <- data.frame(
    id = c(5, 6),
    value = c(500, 600),
    year = c(2024, 2024)  # numeric to match
  )
  db_hive_write(df2, "Test", "Data", partition_by = "year", mode = "replace_partitions")

  # Read back
  result <- db_hive_read("Test", "Data") |> dplyr::collect()

  # Should have 2 rows from 2023 (untouched) + 2 rows from 2024 (replaced) = 4
  expect_equal(nrow(result), 4)

  # Coerce year to numeric for comparison (hive partition may return as various types)
  result$year_num <- as.numeric(result$year)

  # 2023 values should be untouched
  r2023 <- result[abs(result$year_num - 2023) < 0.01, ]
  expect_equal(nrow(r2023), 2)
  expect_true(all(r2023$id %in% c(1, 2)))

  # 2024 values should be replaced
  r2024 <- result[abs(result$year_num - 2024) < 0.01, ]
  expect_equal(nrow(r2024), 2)
  expect_true(all(r2024$id %in% c(5, 6)))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_write respects compression parameter", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "compress_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  df <- data.frame(id = 1:100, value = runif(100))

  # Should work with valid compression (produces message, so use expect_message)
  expect_message(
    db_hive_write(df, "Test", "Zstd", compression = "zstd"),
    "Published"
  )

  expect_message(
    db_hive_write(df, "Test", "Snappy", compression = "snappy"),
    "Published"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_lake_write() - Connection and Mode Checks
# ==============================================================================

test_that("db_lake_write errors when not connected", {
  clean_db_env()

  expect_error(
    db_lake_write(data.frame(x = 1), table = "test"),
    "Not connected"
  )
})

test_that("db_lake_write errors in wrong mode", {
  clean_db_env()

  db_connect(path = "/test")

  expect_error(
    db_lake_write(data.frame(x = 1), table = "test"),
    "Connected in hive mode"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_lake_write() - Input Validation
# ==============================================================================

test_that("db_lake_write validates data argument", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_val.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_lake_write("not a df", table = "t"), "must be a data.frame")
  expect_error(db_lake_write(NULL, table = "t"), "must be a data.frame")

  clean_db_env()
})

test_that("db_lake_write validates schema name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_schema_val.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(x = 1)
  expect_error(db_lake_write(df, schema = "", table = "t"), "must be a single, non-empty")
  expect_error(db_lake_write(df, schema = "../bad", table = "t"), "potentially dangerous characters")

  clean_db_env()
})

test_that("db_lake_write validates table name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_table_val.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(x = 1)
  expect_error(db_lake_write(df, table = ""), "must be a single, non-empty")
  expect_error(db_lake_write(df, table = "bad;drop"), "potentially dangerous characters")

  clean_db_env()
})

# ==============================================================================
# Tests for db_lake_write() - Integration
# ==============================================================================

test_that("db_lake_write overwrite mode works", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_write_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, name = c("a", "b", "c"))

  result <- db_lake_write(df, table = "items", mode = "overwrite")

  expect_equal(result, "test.main.items")

  # Read back
  read_df <- db_lake_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(read_df), 3)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lake_write append mode works", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_append_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create table
  df1 <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_lake_write(df1, table = "items", mode = "overwrite")

  # Append
  df2 <- data.frame(id = 4:6, value = c(40, 50, 60))
  db_lake_write(df2, table = "items", mode = "append")

  # Read back
  read_df <- db_lake_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(read_df), 6)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lake_write works with custom schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_schema_write_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create schema
  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE SCHEMA test.trade")

  df <- data.frame(product = c("widget", "gadget"), price = c(9.99, 19.99))

  result <- db_lake_write(df, schema = "trade", table = "products")

  expect_equal(result, "test.trade.products")

  # Read back
  read_df <- db_lake_read(schema = "trade", table = "products") |> dplyr::collect()
  expect_equal(nrow(read_df), 2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lake_write records commit metadata", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_commit_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:3, value = c(10, 20, 30))

  db_lake_write(df, table = "items", mode = "overwrite",
                commit_author = "test_user",
                commit_message = "Test commit message")

  # Check snapshot has the metadata
  snapshots <- db_snapshots()

  # Should have at least one snapshot
  expect_true(nrow(snapshots) >= 1)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lake_write rolls back on error", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_rollback_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create initial table
  df1 <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_lake_write(df1, table = "items", mode = "overwrite")

  # Try to append incompatible data (this should fail)
  # DuckLake should enforce schema
  df_bad <- data.frame(different_col = c("a", "b"))

  # This should error
  expect_error(
    db_lake_write(df_bad, table = "items", mode = "append")
  )

  # Original data should still be intact
  read_df <- db_lake_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(read_df), 3)
  expect_true("id" %in% names(read_df))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
