# tests/testthat/test-db_read.R

# ==============================================================================
# Tests for .db_dataset_glob()
# ==============================================================================

test_that(".db_dataset_glob builds correct path", {
  glob <- datapond:::.db_dataset_glob

  expect_equal(
    glob("/data", "Trade", "Imports"),
    "/data/Trade/Imports/**/*.parquet"
  )

  expect_equal(
    glob("//CSO-NAS/DataLake", "Labour", "Employment"),
    "//CSO-NAS/DataLake/Labour/Employment/**/*.parquet"
  )

  # Windows-style paths
  expect_equal(
    glob("C:/data/lake", "Health", "Hospitals"),
    "C:/data/lake/Health/Hospitals/**/*.parquet"
  )
})

# ==============================================================================
# Tests for db_hive_read() - Connection and Mode Checks
# ==============================================================================

test_that("db_hive_read errors when not connected", {
  clean_db_env()

  expect_error(
    db_hive_read("Trade", "Imports"),
    "Not connected"
  )
})

test_that("db_hive_read errors in wrong mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Connect in DuckLake mode
  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_mode.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_hive_read("Trade", "Imports"),
    "Connected in DuckLake mode"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_hive_read() - Input Validation
# ==============================================================================

test_that("db_hive_read validates section name", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_hive_read("", "Imports"), "must be a single, non-empty string")
  expect_error(db_hive_read("../bad", "Imports"), "potentially dangerous characters")
  expect_error(db_hive_read("Trade/Sub", "Imports"), "contains invalid characters")
  expect_error(db_hive_read(123, "Imports"), "must be a single, non-empty string")
  expect_error(db_hive_read(NULL, "Imports"), "must be a single, non-empty string")

  clean_db_env()
})

test_that("db_hive_read validates dataset name", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_hive_read("Trade", ""), "must be a single, non-empty string")
  expect_error(db_hive_read("Trade", "../etc"), "potentially dangerous characters")
  expect_error(db_hive_read("Trade", "Im ports"), "contains invalid characters")
  expect_error(db_hive_read("Trade", c("a", "b")), "must be a single, non-empty string")

  clean_db_env()
})

# ==============================================================================
# Tests for db_hive_read() - Options Handling (...)
# ==============================================================================

test_that("db_hive_read requires named arguments in ...", {
  clean_db_env()
  db_connect(path = tempdir())

  # Unnamed arguments should error
  expect_error(
    db_hive_read("Trade", "Imports", TRUE),
    "must be named"
  )

  expect_error(
    db_hive_read("Trade", "Imports", "value"),
    "must be named"
  )

  clean_db_env()
})

test_that("db_hive_read rejects unsupported option types", {
  clean_db_env()
  db_connect(path = tempdir())

  # List not supported
  expect_error(
    db_hive_read("Trade", "Imports", opt = list(a = 1)),
    "Unsupported option type"
  )

  # Multiple strings not supported
  expect_error(
    db_hive_read("Trade", "Imports", cols = c("a", "b")),
    "Unsupported option type"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_hive_read() - Integration with Real Data
# ==============================================================================

test_that("db_hive_read works with actual parquet files", {
  clean_db_env()

  # Create temp directory with parquet file
  temp_dir <- tempfile(pattern = "hive_test_")
  dir.create(file.path(temp_dir, "Trade", "Imports"), recursive = TRUE)

  # Write test parquet file
  test_data <- data.frame(
    id = 1:5,
    value = c(100, 200, 300, 400, 500),
    country = c("IE", "UK", "DE", "FR", "ES")
  )

  arrow::write_parquet(
    test_data,
    file.path(temp_dir, "Trade", "Imports", "data.parquet")
  )

  # Connect and read
  db_connect(path = temp_dir)

  result <- db_hive_read("Trade", "Imports")

  # Should return a lazy tbl
 expect_s3_class(result, "tbl_lazy")

  # Collect and check
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 5)
  expect_true("id" %in% names(collected))
  expect_true("value" %in% names(collected))
  expect_true("country" %in% names(collected))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_read works with hive partitioned data", {
  skip_if_not_installed("arrow")
  clean_db_env()

  # Create temp directory with partitioned structure
  temp_dir <- tempfile(pattern = "hive_part_test_")

  # Create partitions
  for (year in c(2023, 2024)) {
    for (month in c(1, 2)) {
      part_dir <- file.path(temp_dir, "Trade", "Imports",
                            paste0("year=", year), paste0("month=", month))
      dir.create(part_dir, recursive = TRUE)

      test_data <- data.frame(
        id = 1:3,
        value = c(100, 200, 300) * year
      )

      arrow::write_parquet(test_data, file.path(part_dir, "data.parquet"))
    }
  }

  # Connect and read
  db_connect(path = temp_dir)

  result <- db_hive_read("Trade", "Imports")
  collected <- dplyr::collect(result)

  # Should have partition columns
  expect_true("year" %in% names(collected))
  expect_true("month" %in% names(collected))

  # Should have all rows (3 rows * 4 partitions = 12)
  expect_equal(nrow(collected), 12)

  # Filter by partition should work
  filtered <- result |>
    dplyr::filter(year == 2024) |>
    dplyr::collect()
  expect_equal(nrow(filtered), 6)  # 3 rows * 2 months

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_read errors for non-existent dataset", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "empty_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  expect_error(
    db_hive_read("NonExistent", "Dataset"),
    "Unable to read dataset"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_hive_read passes options correctly", {
  skip_if_not_installed("arrow")
  clean_db_env()

  # Create test data with a filename we can verify
  temp_dir <- tempfile(pattern = "opts_test_")
  dir.create(file.path(temp_dir, "Test", "Data"), recursive = TRUE)

  test_data <- data.frame(id = 1:3, value = c(10, 20, 30))
  arrow::write_parquet(test_data, file.path(temp_dir, "Test", "Data", "myfile.parquet"))

  db_connect(path = temp_dir)

  # Request filename column
  result <- db_hive_read("Test", "Data", filename = TRUE)
  collected <- dplyr::collect(result)

  expect_true("filename" %in% names(collected))
  expect_true(any(grepl("myfile.parquet", collected$filename)))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_lake_read() - Connection and Mode Checks
# ==============================================================================

test_that("db_lake_read errors when not connected", {
  clean_db_env()

  expect_error(
    db_lake_read(table = "imports"),
    "Not connected"
  )
})

test_that("db_lake_read errors in wrong mode", {
  clean_db_env()

  # Connect in hive mode
  db_connect(path = "/test")

  expect_error(
    db_lake_read(table = "imports"),
    "Connected in hive mode"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_lake_read() - Input Validation
# ==============================================================================

test_that("db_lake_read validates schema name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_schema.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_lake_read(schema = "", table = "t"), "must be a single, non-empty string")
  expect_error(db_lake_read(schema = "../bad", table = "t"), "potentially dangerous characters")
  expect_error(db_lake_read(schema = 123, table = "t"), "must be a single, non-empty string")

  clean_db_env()
})

test_that("db_lake_read validates table name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_table.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_lake_read(table = ""), "must be a single, non-empty string")
  expect_error(db_lake_read(table = "bad;drop"), "potentially dangerous characters")
  expect_error(db_lake_read(table = NULL), "must be a single, non-empty string")

  clean_db_env()
})

test_that("db_lake_read rejects both version and timestamp", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_tt.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_lake_read(table = "test", version = 1, timestamp = "2025-01-01"),
    "only one of"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_lake_read() - Integration with DuckLake
# ==============================================================================

test_that("db_lake_read works with DuckLake table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Setup
  temp_dir <- tempfile(pattern = "lake_read_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create a table
  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'Widget'), (2, 'Gadget')")

  # Read it back
  result <- db_lake_read(schema = "main", table = "products")

  expect_s3_class(result, "tbl_lazy")

  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 2)
  expect_true("id" %in% names(collected))
  expect_true("name" %in% names(collected))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lake_read time travel by version works", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  # Setup
  temp_dir <- tempfile(pattern = "lake_tt_test_")
  dir.create(temp_dir)

  db_lake_connect(
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
  current <- db_lake_read(table = "items") |> dplyr::collect()
  expect_equal(nrow(current), 2)

  # Version 1 should have 1 row
  v1_data <- db_lake_read(table = "items", version = v1) |> dplyr::collect()
  expect_equal(nrow(v1_data), 1)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lake_read errors for non-existent table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_err_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_lake_read(table = "nonexistent"),
    "Unable to read table"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lake_read works with different schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_schema_test_")
  dir.create(temp_dir)

  db_lake_connect(
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
  result <- db_lake_read(schema = "trade", table = "imports")
  collected <- dplyr::collect(result)

  expect_equal(nrow(collected), 2)
  expect_equal(sum(collected$value), 301.25)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
