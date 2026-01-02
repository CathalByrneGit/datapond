# tests/testthat/test-preview.R

# ==============================================================================
# Tests for db_preview_hive_write()
# ==============================================================================

test_that("db_preview_hive_write errors when not connected", {
  clean_db_env()
  
  df <- data.frame(id = 1:3)
  expect_error(db_preview_hive_write(df, "Trade", "Imports"), "Not connected")
})

test_that("db_preview_hive_write validates inputs", {
  clean_db_env()
  db_connect(path = tempdir())
  
  expect_error(db_preview_hive_write("not a df", "Trade", "Test"), "data.frame")
  expect_error(db_preview_hive_write(data.frame(x=1), "", "Test"), "non-empty")
  
  clean_db_env()
})

test_that("db_preview_hive_write shows preview for new dataset", {
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "preview_new_")
  dir.create(temp_dir)
  
  db_connect(path = temp_dir)
  
  df <- data.frame(id = 1:10, value = runif(10))
  
  expect_output(
    preview <- db_preview_hive_write(df, "Trade", "NewDataset"),
    "WRITE PREVIEW"
  )
  
  expect_false(preview$target_exists)
  expect_equal(preview$incoming$rows, 10)
  expect_equal(preview$incoming$cols, 2)
  
  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_preview_hive_write shows schema changes", {
  skip_if_not_installed("arrow")
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "preview_schema_")
  dir.create(temp_dir)
  
  db_connect(path = temp_dir)
  con <- csolake:::.db_get_con()
  
  # Create existing dataset
  path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(path, recursive = TRUE)
  
  df1 <- data.frame(id = 1:3, old_col = c("a", "b", "c"))
  duckdb::duckdb_register(con, "df1", df1)
  DBI::dbExecute(con, sprintf("COPY df1 TO '%s' (FORMAT PARQUET)", file.path(path, "data.parquet")))
  duckdb::duckdb_unregister(con, "df1")
  
  # Preview with different schema
  df2 <- data.frame(id = 1:5, new_col = 1:5)
  
  expect_output(
    preview <- db_preview_hive_write(df2, "Trade", "Imports"),
    "Schema Changes"
  )
  
  expect_true(preview$target_exists)
  expect_true("new_col" %in% preview$schema_changes$new_columns)
  expect_true("old_col" %in% preview$schema_changes$removed_columns)
  
  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_preview_hive_write shows partition impact", {
  skip_if_not_installed("arrow")
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "preview_part_")
  dir.create(temp_dir)
  
  db_connect(path = temp_dir)
  
  df <- data.frame(
    year = c(2023L, 2023L, 2024L, 2024L),
    month = c(1L, 2L, 1L, 2L),
    value = 1:4
  )
  
  expect_output(
    preview <- db_preview_hive_write(df, "Trade", "Test", 
                                      partition_by = c("year", "month")),
    "Partition Impact"
  )
  
  expect_equal(preview$partition_impact$partition_by, c("year", "month"))
  expect_equal(preview$partition_impact$partitions_in_data, 4)
  
  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_preview_lake_write()
# ==============================================================================

test_that("db_preview_lake_write errors when not connected", {
  clean_db_env()
  
  df <- data.frame(id = 1:3)
  expect_error(db_preview_lake_write(df, table = "test"), "Not connected")
})

test_that("db_preview_lake_write shows preview for new table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "lake_preview_")
  dir.create(temp_dir)
  
  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )
  
  df <- data.frame(id = 1:10, name = letters[1:10])
  
  expect_output(
    preview <- db_preview_lake_write(df, table = "products"),
    "DUCKLAKE WRITE PREVIEW"
  )
  
  expect_false(preview$table_exists)
  expect_equal(preview$incoming$rows, 10)
  
  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_preview_lake_write shows append warning for non-existent table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()
  
  temp_dir <- tempfile(pattern = "lake_append_")
  dir.create(temp_dir)
  
  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )
  
  df <- data.frame(id = 1:5)
  
  expect_output(
    preview <- db_preview_lake_write(df, table = "nonexistent", mode = "append"),
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
  
  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )
  
  con <- csolake:::.db_get_con()
  
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
  
  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )
  
  con <- csolake:::.db_get_con()
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
  
  db_lake_connect(
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
