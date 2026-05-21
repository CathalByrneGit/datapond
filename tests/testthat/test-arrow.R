# tests/testthat/test-arrow.R
# Tests for Arrow integration functions

# ==============================================================================
# Tests for db_read_arrow()
# ==============================================================================

test_that("db_read_arrow errors when not connected", {
  clean_db_env()
  expect_error(db_read_arrow(table = "test"), "Not connected")
})

test_that("db_read_arrow errors when arrow package not available", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if(requireNamespace("arrow", quietly = TRUE), "Test requires arrow to NOT be installed")

  clean_db_env()
  lake <- create_test_lake("arrow_test")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  expect_error(db_read_arrow(table = "test"), "arrow.*package.*required")

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_read_arrow reads table as data.frame by default", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_not(requireNamespace("arrow", quietly = TRUE), "arrow package not available")

  clean_db_env()
  lake <- create_test_lake("arrow_read_df")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Create test data
  test_data <- data.frame(
    id = 1:5,
    value = c(10.5, 20.5, 30.5, 40.5, 50.5),
    name = c("a", "b", "c", "d", "e")
  )
  db_write(test_data, table = "test_table")

  # Read via Arrow
  result <- db_read_arrow(table = "test_table")

  expect_true(is.data.frame(result))
  expect_equal(nrow(result), 5)
  expect_true("id" %in% names(result))
  expect_true("value" %in% names(result))

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_read_arrow reads table as Arrow Table when as_data_frame=FALSE", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_not(requireNamespace("arrow", quietly = TRUE), "arrow package not available")

  clean_db_env()
  lake <- create_test_lake("arrow_read_tbl")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  test_data <- data.frame(id = 1:3, value = c(1.1, 2.2, 3.3))
  db_write(test_data, table = "test_table")

  result <- db_read_arrow(table = "test_table", as_data_frame = FALSE)

  expect_true(inherits(result, "ArrowTabular") || inherits(result, "Table"))

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_read_arrow reads specific columns", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_not(requireNamespace("arrow", quietly = TRUE), "arrow package not available")

 clean_db_env()
  lake <- create_test_lake("arrow_read_cols")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  test_data <- data.frame(id = 1:3, value = c(1, 2, 3), name = c("a", "b", "c"))
  db_write(test_data, table = "test_table")

  result <- db_read_arrow(table = "test_table", columns = c("id", "name"))

  expect_true("id" %in% names(result))
  expect_true("name" %in% names(result))
  # value column should not be present
  expect_false("value" %in% names(result))

  clean_db_env()
  cleanup_test_lake(lake)
})

# ==============================================================================
# Tests for db_write_arrow()
# ==============================================================================

test_that("db_write_arrow errors when not connected", {
  clean_db_env()
  expect_error(db_write_arrow(data.frame(x = 1), table = "test"), "Not connected")
})

test_that("db_write_arrow writes data.frame", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_not(requireNamespace("arrow", quietly = TRUE), "arrow package not available")

  clean_db_env()
  lake <- create_test_lake("arrow_write_df")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  test_data <- data.frame(id = 1:3, value = c(10, 20, 30))
  db_write_arrow(test_data, table = "test_table")

  # Verify data was written
  result <- db_read(table = "test_table") |> dplyr::collect()
  expect_equal(nrow(result), 3)

  clean_db_env()
  cleanup_test_lake(lake)
})

test_that("db_write_arrow writes Arrow Table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip_if_not(requireNamespace("arrow", quietly = TRUE), "arrow package not available")

  clean_db_env()
  lake <- create_test_lake("arrow_write_tbl")

  db_connect(
    catalog = "test",
    metadata_path = lake$metadata_path,
    data_path = lake$data_path
  )

  # Create Arrow Table
  arrow_tbl <- arrow::arrow_table(id = 1:5, value = c(1.1, 2.2, 3.3, 4.4, 5.5))
  db_write_arrow(arrow_tbl, table = "test_table")

  # Verify data was written
  result <- db_read(table = "test_table") |> dplyr::collect()
  expect_equal(nrow(result), 5)

  clean_db_env()
  cleanup_test_lake(lake)
})
