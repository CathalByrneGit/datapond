# tests/testthat/test-discovery.R

# ==============================================================================
# Tests for db_list_schemas() - DuckLake
# ==============================================================================

test_that("db_list_schemas errors when not connected", {
  clean_db_env()

  expect_error(db_list_schemas(), "Not connected")
})

test_that("db_list_schemas returns schema names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "schemas_test_")
  dir.create(temp_dir)

  db_connect(
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
# Tests for db_tables() - DuckLake
# ==============================================================================

test_that("db_tables errors when not connected", {
  clean_db_env()

  expect_error(db_tables(), "Not connected")
})

test_that("db_tables validates schema name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_val.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_tables(""), "non-empty")
  expect_error(db_tables("test/schema"), "invalid characters")

  clean_db_env()
})

test_that("db_tables returns table names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "tables_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create tables
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE test.main.orders (id INTEGER)")

  tables <- db_tables()

  expect_true("products" %in% tables)
  expect_true("orders" %in% tables)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_tables filters by schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "schema_filter_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create tables in different schemas
  DBI::dbExecute(con, "CREATE TABLE test.main.main_table (id INTEGER)")
  DBI::dbExecute(con, "CREATE SCHEMA test.other")
  DBI::dbExecute(con, "CREATE TABLE test.other.other_table (id INTEGER)")

  main_tables <- db_tables("main")
  other_tables <- db_tables("other")

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

test_that("db_list_views returns view names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "views_test_")
  dir.create(temp_dir)

  db_connect(
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

test_that("db_table_exists validates names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
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

  db_connect(
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

  db_connect(
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

test_that("db_create_schema validates name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
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

  db_connect(
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

  db_connect(
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

  db_connect(
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

test_that("db_set_partitioning validates inputs", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "partition_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Table doesn't exist
  expect_error(db_set_partitioning("main", "nonexistent", c("year")), "does not exist")

  # Invalid partition_by type
  db_write(data.frame(x = 1, year = 2024), schema = "main", table = "test_tbl")
  expect_error(db_set_partitioning("main", "test_tbl", 123), "character vector")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_set_partitioning sets partition keys on table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "partition_set_test_")
  dir.create(temp_dir)

  db_connect(
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
  db_write(test_data, schema = "main", table = "partitioned_tbl")

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

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create and partition a table
  test_data <- data.frame(year = 2024, value = 100)
  db_write(test_data, schema = "main", table = "remove_part_tbl")
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

test_that("db_get_partitioning returns NULL for non-partitioned table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "get_partition_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create a table without partitioning
  db_write(data.frame(x = 1), schema = "main", table = "no_part_tbl")

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

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create and partition a table
  test_data <- data.frame(year = 2024, month = 1, value = 100)
  db_write(test_data, schema = "main", table = "get_part_tbl")
  db_set_partitioning("main", "get_part_tbl", c("year", "month"))

  result <- db_get_partitioning("main", "get_part_tbl")
  expect_true("year" %in% result)
  expect_true("month" %in% result)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_create_view() - DuckLake
# ==============================================================================

test_that("db_create_view errors when not connected", {
  clean_db_env()

  expect_error(db_create_view(view = "test", query = "SELECT 1"), "Not connected")
})

test_that("db_create_view validates inputs", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "view_validation_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_create_view(view = "", query = "SELECT 1"), "non-empty")
  expect_error(db_create_view(view = "test", query = ""), "non-empty")
  expect_error(db_create_view(view = "test", query = NULL), "non-empty")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_create_view creates a view", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "create_view_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create a table first
  db_write(data.frame(id = 1:3, active = c(TRUE, FALSE, TRUE)),
           schema = "main", table = "users")

  # Create view
  expect_message(
    db_create_view(view = "active_users",
                   query = "SELECT * FROM test.main.users WHERE active = true"),
    "Created view"
  )

  # Check view exists
  views <- db_list_views()
  expect_true("active_users" %in% views)

  # Check view works
  result <- DBI::dbGetQuery(datapond:::.db_get_con(),
                            "SELECT * FROM test.main.active_users")
  expect_equal(nrow(result), 2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_create_view replaces existing view", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "replace_view_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create view
  db_create_view(view = "my_view", query = "SELECT 1 AS value")

  # Replace without flag should error
  expect_error(db_create_view(view = "my_view", query = "SELECT 2 AS value"))

  # Replace with flag should succeed
  expect_message(
    db_create_view(view = "my_view", query = "SELECT 2 AS value", replace = TRUE),
    "Created view"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_drop_view() - DuckLake
# ==============================================================================

test_that("db_drop_view errors when not connected", {
  clean_db_env()

  expect_error(db_drop_view(view = "test"), "Not connected")
})

test_that("db_drop_view drops a view", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "drop_view_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create and drop view
  db_create_view(view = "temp_view", query = "SELECT 1")
  expect_true("temp_view" %in% db_list_views())

  expect_message(db_drop_view(view = "temp_view"), "Dropped view")
  expect_false("temp_view" %in% db_list_views())

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_drop_view with if_exists handles missing view", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "drop_view_if_exists_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Without if_exists - should error
  expect_error(db_drop_view(view = "nonexistent"))

  # With if_exists - should succeed silently
  expect_message(db_drop_view(view = "nonexistent", if_exists = TRUE), "Dropped view")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_create_macro() - DuckLake
# ==============================================================================

test_that("db_create_macro errors when not connected", {
  clean_db_env()

  expect_error(db_create_macro(name = "test", body = "1 + 1"), "Not connected")
})

test_that("db_create_macro validates inputs", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "macro_validation_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_create_macro(name = "", body = "1"), "non-empty")
  expect_error(db_create_macro(name = "test", body = ""), "non-empty")
  expect_error(db_create_macro(name = "test", body = NULL), "non-empty")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_create_macro creates scalar macro", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "scalar_macro_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create scalar macro with parameters
  expect_message(
    db_create_macro(name = "add_vals", params = c("a", "b"), body = "a + b"),
    "Created macro"
  )

  # Use the macro
  result <- DBI::dbGetQuery(datapond:::.db_get_con(),
                            "SELECT test.main.add_vals(10, 20) AS result")
  expect_equal(result$result, 30)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_create_macro creates table macro", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "table_macro_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create a table first
  db_write(data.frame(id = 1:5, value = c(10, 20, 30, 40, 50)),
           schema = "main", table = "numbers")

  # Create table macro
  expect_message(
    db_create_macro(
      name = "big_numbers",
      params = c("threshold"),
      body = "SELECT * FROM test.main.numbers WHERE value > threshold",
      table_macro = TRUE
    ),
    "Created macro"
  )

  # Use the table macro
  result <- DBI::dbGetQuery(datapond:::.db_get_con(),
                            "SELECT * FROM test.main.big_numbers(25)")
  expect_equal(nrow(result), 3)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_create_macro with typed parameters", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "typed_macro_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create macro with typed parameters
  expect_message(
    db_create_macro(
      name = "typed_add",
      params = c(a = "INTEGER", b = "INTEGER"),
      body = "a + b"
    ),
    "Created macro"
  )

  result <- DBI::dbGetQuery(datapond:::.db_get_con(),
                            "SELECT test.main.typed_add(5, 3) AS result")
  expect_equal(result$result, 8)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_list_macros() - DuckLake
# ==============================================================================

test_that("db_list_macros errors when not connected", {
  clean_db_env()

  expect_error(db_list_macros(), "Not connected")
})

test_that("db_list_macros returns macro names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "list_macros_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create some macros
  db_create_macro(name = "macro_one", body = "1")
  db_create_macro(name = "macro_two", body = "2")

  macros <- db_list_macros()
  expect_true("macro_one" %in% macros)
  expect_true("macro_two" %in% macros)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_drop_macro() - DuckLake
# ==============================================================================

test_that("db_drop_macro errors when not connected", {
  clean_db_env()

  expect_error(db_drop_macro(name = "test"), "Not connected")
})

test_that("db_drop_macro drops a macro", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  skip("Known issue: DuckLake metadata may not update immediately after DROP MACRO")
  clean_db_env()

  temp_dir <- tempfile(pattern = "drop_macro_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create and drop macro
  db_create_macro(name = "temp_macro", body = "42")
  expect_true("temp_macro" %in% db_list_macros())

  expect_message(db_drop_macro(name = "temp_macro"), "Dropped macro")
  expect_false("temp_macro" %in% db_list_macros())

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_drop_macro with if_exists handles missing macro", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "drop_macro_if_exists_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Without if_exists - should error
  expect_error(db_drop_macro(name = "nonexistent"))

  # With if_exists - should succeed
  expect_message(db_drop_macro(name = "nonexistent", if_exists = TRUE), "Dropped macro")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_comment() - DuckLake
# ==============================================================================

test_that("db_comment errors when not connected", {
  clean_db_env()

  expect_error(db_comment(table = "test", comment = "A comment"), "Not connected")
})

test_that("db_comment adds table comment", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "table_comment_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create table
  db_write(data.frame(id = 1:3), schema = "main", table = "commented_tbl")

  # Add comment
  expect_message(
    db_comment(table = "commented_tbl", comment = "This is a test table"),
    "Set comment on table"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_comment adds column comment", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "column_comment_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create table
  db_write(data.frame(id = 1:3, value = c(10, 20, 30)),
           schema = "main", table = "col_comment_tbl")

  # Add column comment
  expect_message(
    db_comment(table = "col_comment_tbl", column = "value",
               comment = "The numeric value"),
    "Set comment on column"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_comment removes comment with NULL", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "remove_comment_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create table and add comment
  db_write(data.frame(id = 1:3), schema = "main", table = "remove_comment_tbl")
  db_comment(table = "remove_comment_tbl", comment = "Initial comment")

  # Remove comment
  expect_message(
    db_comment(table = "remove_comment_tbl", comment = NULL),
    "Set comment on table"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_enable_logging() - DuckLake
# ==============================================================================

test_that("db_enable_logging errors when not connected", {
  clean_db_env()

  expect_error(db_enable_logging(), "Not connected")
})

test_that("db_enable_logging enables and disables logging", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "logging_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Enable logging
  expect_message(db_enable_logging(TRUE), "Logging enabled")

  # Disable logging
  expect_message(db_enable_logging(FALSE), "Logging disabled")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_enable_logging accepts log_type argument", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "logging_type_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  expect_message(db_enable_logging(TRUE, log_type = "query"), "query")
  expect_message(db_enable_logging(TRUE, log_type = "metadata"), "metadata")
  expect_message(db_enable_logging(TRUE, log_type = "all"), "all")

  db_enable_logging(FALSE)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
