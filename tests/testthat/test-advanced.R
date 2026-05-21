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

# ==============================================================================
# Tests for db_set_clustering()
# ==============================================================================

test_that("db_set_clustering errors when not connected", {
  clean_db_env()
  expect_error(db_set_clustering(table = "test", columns = "id"), "Not connected")
})

test_that("db_set_clustering sets clustering on table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")

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
  expect_message(db_set_clustering(table = "test_table", columns = c("date", "id")), "clustering")

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_set_clustering removes clustering with NULL columns", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")

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
  expect_message(db_set_clustering(table = "test_table", columns = NULL), "Removed|clustering")

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

# ==============================================================================
# Tests for db_iceberg_metadata()
# ==============================================================================

test_that("db_iceberg_metadata errors when not connected", {
  clean_db_env()
  expect_error(db_iceberg_metadata(table = "test"), "Not connected")
})
