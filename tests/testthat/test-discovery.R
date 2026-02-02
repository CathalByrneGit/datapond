# tests/testthat/test-discovery.R

# ==============================================================================
# Tests for db_list_sections() - Hive
# ==============================================================================

test_that("db_list_sections errors when not connected", {
  clean_db_env()

  expect_error(db_list_sections(), "Not connected")
})

test_that("db_list_sections errors in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_mode.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_list_sections(), "DuckLake mode")

  clean_db_env()
})

test_that("db_list_sections returns section folders", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "sections_test_")
  dir.create(temp_dir)

  # Create some section folders
  dir.create(file.path(temp_dir, "Trade"))
  dir.create(file.path(temp_dir, "Labour"))
  dir.create(file.path(temp_dir, "Health"))
  dir.create(file.path(temp_dir, ".hidden"))  # Should be filtered out

  db_connect(path = temp_dir)

  sections <- db_list_sections()

  expect_equal(sort(sections), c("Health", "Labour", "Trade"))
  expect_false(".hidden" %in% sections)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_list_sections handles empty directory", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "empty_sections_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  sections <- db_list_sections()

  expect_equal(length(sections), 0)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_list_sections errors for non-existent path", {
  clean_db_env()

  # Connect with a path that will be deleted
  temp_dir <- tempfile(pattern = "delete_me_")
  dir.create(temp_dir)
  db_connect(path = temp_dir)

  # Delete the path
  unlink(temp_dir, recursive = TRUE)

  expect_error(db_list_sections(), "does not exist")

  clean_db_env()
})

# ==============================================================================
# Tests for db_list_datasets() - Hive
# ==============================================================================

test_that("db_list_datasets errors when not connected", {
  clean_db_env()

  expect_error(db_list_datasets("Trade"), "Not connected")
})

test_that("db_list_datasets errors in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_mode2.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_list_datasets("Trade"), "DuckLake mode")

  clean_db_env()
})

test_that("db_list_datasets validates section name", {
  clean_db_env()
  db_connect(path = tempdir())

  expect_error(db_list_datasets(""), "non-empty")
  expect_error(db_list_datasets("Trade/Imports"), "invalid characters")
  expect_error(db_list_datasets("../etc"), "potentially dangerous")

  clean_db_env()
})

test_that("db_list_datasets returns dataset folders", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "datasets_test_")
  dir.create(temp_dir)

  # Create section with datasets
  section_path <- file.path(temp_dir, "Trade")
  dir.create(section_path)
  dir.create(file.path(section_path, "Imports"))
  dir.create(file.path(section_path, "Exports"))
  dir.create(file.path(section_path, ".hidden"))  # Should be filtered
  dir.create(file.path(section_path, "year=2024"))  # Partition folder, should be filtered

  db_connect(path = temp_dir)

  datasets <- db_list_datasets("Trade")

  expect_equal(sort(datasets), c("Exports", "Imports"))
  expect_false(".hidden" %in% datasets)
  expect_false("year=2024" %in% datasets)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_list_datasets errors for non-existent section", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "no_section_test_")
  dir.create(temp_dir)
  db_connect(path = temp_dir)

  expect_error(db_list_datasets("NonExistent"), "does not exist")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_dataset_exists() - Hive
# ==============================================================================

test_that("db_dataset_exists errors when not connected", {
  clean_db_env()

  expect_error(db_dataset_exists("Trade", "Imports"), "Not connected")
})

test_that("db_dataset_exists errors in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_mode3.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_dataset_exists("Trade", "Imports"), "DuckLake mode")

  clean_db_env()
})

test_that("db_dataset_exists validates names", {
  clean_db_env()
  db_connect(path = tempdir())

  expect_error(db_dataset_exists("", "test"), "non-empty")
  expect_error(db_dataset_exists("test", ""), "non-empty")

  clean_db_env()
})

test_that("db_dataset_exists returns TRUE for existing dataset with parquet files", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "exists_test_")
  dir.create(temp_dir)

  # Create dataset with parquet file
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Write a parquet file
  df <- data.frame(id = 1:3)
  duckdb::duckdb_register(con, "df", df)
  DBI::dbExecute(con, sprintf(
    "COPY df TO '%s' (FORMAT PARQUET)",
    file.path(dataset_path, "data.parquet")
  ))
  duckdb::duckdb_unregister(con, "df")

  expect_true(db_dataset_exists("Trade", "Imports"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dataset_exists returns FALSE for empty directory", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "empty_dataset_test_")
  dir.create(temp_dir)

  # Create empty dataset folder (no parquet files)
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  expect_false(db_dataset_exists("Trade", "Imports"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dataset_exists returns FALSE for non-existent directory", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "nonexist_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  expect_false(db_dataset_exists("Trade", "NonExistent"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dataset_exists finds parquet files in partition subfolders", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "partition_exists_test_")
  dir.create(temp_dir)

  # Create partitioned dataset
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  partition_path <- file.path(dataset_path, "year=2024")
  dir.create(partition_path, recursive = TRUE)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Write parquet file in partition subfolder
  df <- data.frame(id = 1:3, value = c(10, 20, 30))
  duckdb::duckdb_register(con, "df", df)
  DBI::dbExecute(con, sprintf(
    "COPY df TO '%s' (FORMAT PARQUET)",
    file.path(partition_path, "data.parquet")
  ))
  duckdb::duckdb_unregister(con, "df")

  expect_true(db_dataset_exists("Trade", "Imports"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_list_schemas() - DuckLake
# ==============================================================================

test_that("db_list_schemas errors when not connected", {
  clean_db_env()

  expect_error(db_list_schemas(), "Not connected")
})

test_that("db_list_schemas errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_list_schemas(), "hive mode")

  clean_db_env()
})

test_that("db_list_schemas returns schema names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "schemas_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create additional schemas
  DBI::dbExecute(con, "CREATE SCHEMA test.trade")
  DBI::dbExecute(con, "CREATE SCHEMA test.labour")

  schemas <- db_list_schemas()

  expect_true("main" %in% schemas)
  expect_true("trade" %in% schemas)
  expect_true("labour" %in% schemas)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_list_tables() - DuckLake
# ==============================================================================

test_that("db_list_tables errors when not connected", {
  clean_db_env()

  expect_error(db_list_tables(), "Not connected")
})

test_that("db_list_tables errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_list_tables(), "hive mode")

  clean_db_env()
})

test_that("db_list_tables validates schema name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_val.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_list_tables(""), "non-empty")
  expect_error(db_list_tables("test/schema"), "invalid characters")

  clean_db_env()
})

test_that("db_list_tables returns table names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "tables_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create tables
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE test.main.orders (id INTEGER)")

  tables <- db_list_tables()

  expect_true("products" %in% tables)
  expect_true("orders" %in% tables)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_list_tables filters by schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "schema_filter_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create tables in different schemas
  DBI::dbExecute(con, "CREATE TABLE test.main.main_table (id INTEGER)")
  DBI::dbExecute(con, "CREATE SCHEMA test.other")
  DBI::dbExecute(con, "CREATE TABLE test.other.other_table (id INTEGER)")

  main_tables <- db_list_tables("main")
  other_tables <- db_list_tables("other")

  expect_true("main_table" %in% main_tables)
  expect_false("other_table" %in% main_tables)

  expect_true("other_table" %in% other_tables)
  expect_false("main_table" %in% other_tables)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_list_views() - DuckLake
# ==============================================================================

test_that("db_list_views errors when not connected", {
  clean_db_env()

  expect_error(db_list_views(), "Not connected")
})

test_that("db_list_views errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_list_views(), "hive mode")

  clean_db_env()
})

test_that("db_list_views returns view names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "views_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create table and views
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, active BOOLEAN)")
  DBI::dbExecute(con, "CREATE VIEW test.main.active_products AS
    SELECT * FROM test.main.products WHERE active = true")
  DBI::dbExecute(con, "CREATE VIEW test.main.product_count AS
    SELECT COUNT(*) as n FROM test.main.products")

  views <- db_list_views()

  expect_true("active_products" %in% views)
  expect_true("product_count" %in% views)
  expect_false("products" %in% views)  # Table, not view

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_table_exists() - DuckLake
# ==============================================================================

test_that("db_table_exists errors when not connected", {
  clean_db_env()

  expect_error(db_table_exists(table = "test"), "Not connected")
})

test_that("db_table_exists errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_table_exists(table = "test"), "hive mode")

  clean_db_env()
})

test_that("db_table_exists validates names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_val2.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_table_exists(table = ""), "non-empty")
  expect_error(db_table_exists(schema = "", table = "test"), "non-empty")

  clean_db_env()
})

test_that("db_table_exists returns TRUE for existing table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "table_exists_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")

  expect_true(db_table_exists(table = "products"))
  expect_false(db_table_exists(table = "nonexistent"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_table_exists respects schema parameter", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "schema_exists_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")
  DBI::dbExecute(con, "CREATE SCHEMA test.other")

  expect_true(db_table_exists(schema = "main", table = "products"))
  expect_false(db_table_exists(schema = "other", table = "products"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_create_schema() - DuckLake
# ==============================================================================

test_that("db_create_schema errors when not connected", {
  clean_db_env()

  expect_error(db_create_schema("test"), "Not connected")
})

test_that("db_create_schema errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_create_schema("test"), "hive mode")

  clean_db_env()
})

test_that("db_create_schema validates name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_val3.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_create_schema(""), "non-empty")
  expect_error(db_create_schema("test/schema"), "invalid characters")

  clean_db_env()
})

test_that("db_create_schema creates new schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "create_schema_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  expect_message(db_create_schema("trade"), "Schema created")

  schemas <- db_list_schemas()
  expect_true("trade" %in% schemas)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_create_schema is idempotent", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "idempotent_schema_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create twice - should not error
  expect_message(db_create_schema("trade"), "Schema created")
  expect_message(db_create_schema("trade"), "Schema created")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_create_schema returns schema name invisibly", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "return_schema_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  result <- db_create_schema("labour")
  expect_equal(result, "labour")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_set_partitioning() - DuckLake
# ==============================================================================

test_that("db_set_partitioning errors when not connected", {
  clean_db_env()

  expect_error(db_set_partitioning("main", "test", c("year")), "Not connected")
})

test_that("db_set_partitioning errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_set_partitioning("main", "test", c("year")), "hive mode")

  clean_db_env()
})

test_that("db_set_partitioning validates inputs", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "partition_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Table doesn't exist
  expect_error(db_set_partitioning("main", "nonexistent", c("year")), "does not exist")

  # Invalid partition_by type
  db_lake_write(data.frame(x = 1, year = 2024), schema = "main", table = "test_tbl")
  expect_error(db_set_partitioning("main", "test_tbl", 123), "character vector")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_set_partitioning sets partition keys on table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "partition_set_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create a table
  test_data <- data.frame(
    year = c(2023, 2023, 2024, 2024),
    month = c(1, 2, 1, 2),
    value = c(100, 200, 300, 400)
  )
  db_lake_write(test_data, schema = "main", table = "partitioned_tbl")

  # Set partitioning
  expect_message(
    db_set_partitioning("main", "partitioned_tbl", c("year", "month")),
    "Set partitioning"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_set_partitioning removes partitioning with NULL", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "partition_remove_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create and partition a table
  test_data <- data.frame(year = 2024, value = 100)
  db_lake_write(test_data, schema = "main", table = "remove_part_tbl")
  db_set_partitioning("main", "remove_part_tbl", c("year"))

  # Remove partitioning
  expect_message(
    db_set_partitioning("main", "remove_part_tbl", NULL),
    "Removed partitioning"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_get_partitioning() - DuckLake
# ==============================================================================

test_that("db_get_partitioning errors when not connected", {
  clean_db_env()

  expect_error(db_get_partitioning("main", "test"), "Not connected")
})

test_that("db_get_partitioning errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(db_get_partitioning("main", "test"), "hive mode")

  clean_db_env()
})

test_that("db_get_partitioning returns NULL for non-partitioned table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "get_partition_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create a table without partitioning
  db_lake_write(data.frame(x = 1), schema = "main", table = "no_part_tbl")

  result <- db_get_partitioning("main", "no_part_tbl")
  expect_null(result)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_get_partitioning returns partition keys", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "get_partition_keys_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create and partition a table
  test_data <- data.frame(year = 2024, month = 1, value = 100)
  db_lake_write(test_data, schema = "main", table = "get_part_tbl")
  db_set_partitioning("main", "get_part_tbl", c("year", "month"))

  result <- db_get_partitioning("main", "get_part_tbl")
  expect_true("year" %in% result)
  expect_true("month" %in% result)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

