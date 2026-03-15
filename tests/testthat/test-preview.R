# tests/testthat/test-preview.R

# ==============================================================================
# Tests for db_preview_write()
# ==============================================================================

test_that("db_preview_write errors when not connected", {
  clean_db_env()

  df <- data.frame(id = 1:3)
  expect_error(db_preview_write(df, table = "test"), "Not connected")
})

test_that("db_preview_write shows preview for new table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_preview_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:10, name = letters[1:10])

  expect_output(
    preview <- db_preview_write(df, table = "products"),
    "WRITE PREVIEW"
  )

  expect_false(preview$table_exists)
  expect_equal(preview$incoming$rows, 10)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_preview_write shows append warning for non-existent table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lake_append_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:5)

  expect_output(
    preview <- db_preview_write(df, table = "nonexistent", mode = "append"),
    "FAIL"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_preview_upsert()
# ==============================================================================

test_that("db_preview_upsert errors when not connected", {
  clean_db_env()

  df <- data.frame(id = 1:3, value = 1:3)
  expect_error(db_preview_upsert(df, table = "test", by = "id"), "Not connected")
})

test_that("db_preview_upsert shows insert/update counts", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "upsert_preview_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create existing table with some data
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'One'), (2, 'Two'), (3, 'Three')")

  # Preview upsert: 2 existing (updates), 2 new (inserts)
  df <- data.frame(
    id = c(2, 3, 4, 5),
    name = c("Two Updated", "Three Updated", "Four", "Five")
  )

  expect_output(
    preview <- db_preview_upsert(df, table = "products", by = "id"),
    "UPSERT PREVIEW"
  )

  expect_equal(preview$impact$updates, 2)  # id 2, 3
  expect_equal(preview$impact$inserts, 2)  # id 4, 5

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_preview_upsert warns about duplicates", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "upsert_dup_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")

  # Data with duplicate keys
  df <- data.frame(
    id = c(1, 1, 2, 2, 3),  # duplicates!
    name = c("A", "B", "C", "D", "E")
  )

  expect_output(
    preview <- db_preview_upsert(df, table = "products", by = "id"),
    "WARNING.*duplicate"
  )

  expect_equal(preview$impact$duplicates_in_incoming, 2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_preview_upsert handles non-existent table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "upsert_new_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1:5, value = 1:5)

  expect_output(
    preview <- db_preview_upsert(df, table = "new_table", by = "id"),
    "Exists: No"
  )

  expect_equal(preview$impact$inserts, 5)
  expect_equal(preview$impact$updates, 0)
  expect_false(preview$table_exists)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
