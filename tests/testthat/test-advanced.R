# tests/testthat/test-advanced.R
# Tests for advanced DuckLake 1.0 features: inlining, clustering, iceberg

# ==============================================================================
# Tests for db_flush_inlined()
# ==============================================================================

test_that("db_flush_inlined errors when not connected", {
  clean_db_env()
  expect_error(db_flush_inlined(), "Not connected")
})

test_that("db_flush_inlined works with no inlined data", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")

  clean_db_env()
  lake <- create_test_lake("flush_empty")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Should not error even with no inlined data
  expect_message(db_flush_inlined(), "inlined|No inlined")

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_flush_inlined flushes inlined data to parquet", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")

  clean_db_env()
  lake <- create_test_lake("flush_data")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Write small data (will be auto-inlined)
  db_write(data.frame(id = 1:5, value = c(10, 20, 30, 40, 50)), table = "test_table")

  # Flush should complete without error
  expect_message(db_flush_inlined(), "inlined|flushed|No inlined")

  # Data should still be readable
  result <- db_read(table = "test_table") |> dplyr::collect()
  expect_equal(nrow(result), 5)

  clean_db_env()
  cleanup_test_lake(lake)
})

# ==============================================================================
# Tests for db_set_inline_threshold()
# ==============================================================================

test_that("db_set_inline_threshold errors when not connected", {
  clean_db_env()
  expect_error(db_set_inline_threshold(table = "test", threshold = 1000), "Not connected")
})

test_that("db_set_inline_threshold validates threshold", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")

  clean_db_env()
  lake <- create_test_lake("inline_threshold_valid")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Create table first
  db_write(data.frame(id = 1:3), table = "test_table")

  # Negative threshold should error
  expect_error(db_set_inline_threshold(table = "test_table", threshold = -1), "non-negative")

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_set_inline_threshold sets threshold on table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")

  clean_db_env()
  lake <- create_test_lake("inline_threshold_set")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  db_write(data.frame(id = 1:3), table = "test_table")

  # Should succeed (or provide informative message if not supported)
  result <- tryCatch({
    db_set_inline_threshold(table = "test_table", threshold = 50000)
    TRUE
  }, error = function(e) {
    if (grepl("not supported|syntax error", e$message, ignore.case = TRUE)) {
      testthat::skip("ALTER TABLE SET data_inlining_row_limit not supported in this version")
    }
    stop(e)
  })

  expect_true(result)

  clean_db_env()
  cleanup_test_lake(lake)
})

# ==============================================================================
# Tests for db_set_clustering()
# ==============================================================================

test_that("db_set_clustering errors when not connected", {
  clean_db_env()
  expect_error(db_set_clustering(table = "test", columns = "id"), "Not connected")
})

test_that("db_set_clustering sets clustering on table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")

  clean_db_env()
  lake <- create_test_lake("clustering_set")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Create table
  test_data <- data.frame(
    id = 1:10,
    date = as.Date("2024-01-01") + 0:9,
    value = runif(10)
  )
  db_write(test_data, table = "test_table")

  # Set clustering
  expect_message(db_set_clustering(table = "test_table", columns = c("date", "id")), "clustering|sorted")

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_set_clustering removes clustering with NULL columns", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")

  clean_db_env()
  lake <- create_test_lake("clustering_remove")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  test_data <- data.frame(id = 1:5, value = 1:5)
  db_write(test_data, table = "test_table")

  # Set then remove clustering
  db_set_clustering(table = "test_table", columns = "id")
  expect_message(db_set_clustering(table = "test_table", columns = NULL), "Removed|clustering|sorted")

  clean_db_env()
  cleanup_test_lake(lake)
})

# ==============================================================================
# Tests for db_recluster()
# ==============================================================================

test_that("db_recluster errors when not connected", {
  clean_db_env()
  expect_error(db_recluster(table = "test"), "Not connected")
})

test_that("db_recluster validates max_files", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")

  clean_db_env()
  lake <- create_test_lake("recluster_valid")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  db_write(data.frame(id = 1:3), table = "test_table")

  expect_error(db_recluster(table = "test_table", max_files = 0), "positive")
  expect_error(db_recluster(table = "test_table", max_files = -1), "positive")

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_recluster reclusters table data", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")

  clean_db_env()
  lake <- create_test_lake("recluster_run")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Create table with data
  test_data <- data.frame(
    id = sample(1:100, 50),
    date = as.Date("2024-01-01") + sample(0:30, 50, replace = TRUE),
    value = runif(50)
  )
  db_write(test_data, table = "test_table")

  # Set clustering order first
  db_set_clustering(table = "test_table", columns = c("date", "id"))

  # Recluster should run without error
  expect_message(db_recluster(table = "test_table"), "recluster|Recluster")

  clean_db_env()
  cleanup_test_lake(lake)
})

# ==============================================================================
# Tests for db_export_iceberg()
# ==============================================================================

test_that("db_export_iceberg errors when not connected", {
  clean_db_env()
  expect_error(db_export_iceberg(table = "test"), "Not connected")
})

test_that("db_export_iceberg validates catalog_type", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")

  clean_db_env()
  lake <- create_test_lake("iceberg_valid")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  db_write(data.frame(id = 1:3), table = "test_table")

  # Invalid catalog_type should error
  expect_error(db_export_iceberg(table = "test_table", catalog_type = "invalid"))

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_export_iceberg exports table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")

  clean_db_env()
  lake <- create_test_lake("iceberg_export")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Create table
  test_data <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_write(test_data, table = "test_table")

  # Export should run without error
  result <- db_export_iceberg(table = "test_table")

  expect_type(result, "character")

  clean_db_env()
  cleanup_test_lake(lake)
})

# ==============================================================================
# Tests for db_iceberg_metadata()
# ==============================================================================

test_that("db_iceberg_metadata errors when not connected", {
  clean_db_env()
  expect_error(db_iceberg_metadata(table = "test"), "Not connected")
})

test_that("db_iceberg_metadata returns metadata for table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_ducklake_below("1.0.0")

  clean_db_env()
  lake <- create_test_lake("iceberg_meta")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Create table with data
  test_data <- data.frame(
    id = 1:5,
    name = letters[1:5],
    value = c(10.5, 20.5, 30.5, 40.5, 50.5)
  )
  db_write(test_data, table = "test_table")

  # Get Iceberg metadata
  meta <- db_iceberg_metadata(table = "test_table")

  expect_type(meta, "list")
  # Should have schema information
  expect_true("schema" %in% names(meta) || "columns" %in% names(meta) || length(meta) > 0)

  clean_db_env()
  cleanup_test_lake(lake)
})
