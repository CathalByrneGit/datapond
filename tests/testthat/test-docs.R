# tests/testthat/test-docs.R

# ==============================================================================
# Tests for db_describe() - Connection Check
# ==============================================================================

test_that("db_describe errors when not connected", {
  clean_db_env()

  expect_error(db_describe(table = "test"), "Not connected")
})

# ==============================================================================
# Tests for db_describe() - DuckLake mode
# ==============================================================================

test_that("db_describe requires table in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_connect(
    metadata_path = file.path(temp_dir, "test_docs.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_describe(description = "Test"), "table is required")

  clean_db_env()
})

test_that("db_describe works in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "docs_lake_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")

  expect_message(
    db_describe(
      table = "products",
      description = "Product catalog",
      owner = "Sales Team",
      tags = c("products", "reference")
    ),
    "Updated metadata"
  )

  # Retrieve and check
  meta <- db_get_docs(table = "products")
  expect_equal(meta$description, "Product catalog")
  expect_equal(meta$owner, "Sales Team")
  expect_equal(meta$tags, c("products", "reference"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_describe_column()
# ==============================================================================

test_that("db_describe_column works in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "col_lake_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, price DOUBLE)")

  db_describe_column(
    table = "products",
    column = "price",
    description = "Product price",
    units = "EUR"
  )

  meta <- db_get_docs(table = "products")
  expect_equal(meta$columns$price$description, "Product price")
  expect_equal(meta$columns$price$units, "EUR")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_get_docs()
# ==============================================================================

test_that("db_get_docs returns empty metadata for undocumented table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "get_docs_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")

  meta <- db_get_docs(table = "products")

  expect_null(meta$description)
  expect_null(meta$owner)
  expect_equal(meta$tags, character(0))
  expect_equal(meta$columns, list())

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_dictionary()
# ==============================================================================

test_that("db_dictionary returns empty data.frame when no tables", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_empty_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  dict <- db_dictionary()

  expect_s3_class(dict, "data.frame")
  expect_equal(nrow(dict), 0)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dictionary works in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_lake_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "CREATE TABLE test.main.orders (id INTEGER, total DOUBLE)")

  db_describe(table = "products", description = "Product list")

  dict <- db_dictionary(include_columns = FALSE)

  expect_equal(nrow(dict), 2)
  expect_true("products" %in% dict$table)
  expect_true("orders" %in% dict$table)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dictionary includes column details when requested", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_cols_lake_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR, price DOUBLE)")

  # Document a column
  db_describe_column(table = "products", column = "price", description = "Price in EUR")

  dict <- db_dictionary(include_columns = TRUE)

  expect_true("column_name" %in% names(dict))
  expect_true(nrow(dict) >= 3)  # At least 3 columns

  price_row <- dict[dict$column_name == "price", ]
  expect_equal(price_row$column_description, "Price in EUR")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_search()
# ==============================================================================

test_that("db_search finds tables by name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.imports (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE test.main.exports (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")

  results <- db_search("port")

  expect_equal(nrow(results), 2)  # imports and exports
  expect_true(all(c("imports", "exports") %in% results$table))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_search finds tables by description", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_desc_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.table1 (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE test.main.table2 (id INTEGER)")

  db_describe(table = "table1", description = "Contains trade data")
  db_describe(table = "table2", description = "Contains labour data")

  results <- db_search("trade", field = "description")

  expect_equal(nrow(results), 1)
  expect_equal(results$table, "table1")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_search finds tables by tags", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_tags_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.table1 (id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE test.main.table2 (id INTEGER)")

  db_describe(table = "table1", tags = c("official", "monthly"))
  db_describe(table = "table2", tags = c("draft", "annual"))

  results <- db_search("official", field = "tags")

  expect_equal(nrow(results), 1)
  expect_equal(results$table, "table1")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_search_columns()
# ==============================================================================

test_that("db_search_columns finds columns across tables", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_cols_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.table1 (id INTEGER, country_code VARCHAR)")
  DBI::dbExecute(con, "CREATE TABLE test.main.table2 (id INTEGER, region_code VARCHAR)")

  results <- db_search_columns("code")

  expect_equal(nrow(results), 2)
  expect_true("country_code" %in% results$column_name)
  expect_true("region_code" %in% results$column_name)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
