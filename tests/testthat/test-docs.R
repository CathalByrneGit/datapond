# tests/testthat/test-docs.R

# ==============================================================================
# Tests for db_describe() - Hive mode
# ==============================================================================

test_that("db_describe errors when not connected", {
  clean_db_env()

  expect_error(db_describe(section = "Trade", dataset = "Imports"), "Not connected")
})

test_that("db_describe requires section and dataset in hive mode", {
  clean_db_env()
  db_connect(path = tempdir())

  expect_error(db_describe(dataset = "Imports"), "section and dataset are required")
  expect_error(db_describe(section = "Trade"), "section and dataset are required")

  clean_db_env()
})

test_that("db_describe creates metadata file in hive mode", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "docs_test_")
  dir.create(temp_dir)

  # Create dataset folder
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  expect_message(
    db_describe(
      section = "Trade",
      dataset = "Imports",
      description = "Monthly import values",
      owner = "Trade Section",
      tags = c("trade", "monthly")
    ),
    "Updated metadata"
  )

  # Check file exists
  meta_path <- file.path(dataset_path, "_metadata.json")
  expect_true(file.exists(meta_path))

  # Check contents
  meta <- jsonlite::fromJSON(meta_path)
  expect_equal(meta$description, "Monthly import values")
  expect_equal(meta$owner, "Trade Section")
  expect_equal(meta$tags, c("trade", "monthly"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_describe updates existing metadata", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "docs_update_test_")
  dir.create(temp_dir)
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Initial description
  db_describe(section = "Trade", dataset = "Imports", description = "First")

  # Update only owner (description should persist)
  db_describe(section = "Trade", dataset = "Imports", owner = "New Owner")

  meta <- db_get_docs(section = "Trade", dataset = "Imports")
  expect_equal(meta$description, "First")
  expect_equal(meta$owner, "New Owner")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_describe() - DuckLake mode
# ==============================================================================

test_that("db_describe requires table in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
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

  db_lake_connect(
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

test_that("db_describe_column works in hive mode", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "col_docs_test_")
  dir.create(temp_dir)
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  expect_message(
    db_describe_column(
      section = "Trade",
      dataset = "Imports",
      column = "value",
      description = "Import value in EUR",
      units = "EUR (thousands)"
    ),
    "Updated column metadata"
  )

  meta <- db_get_docs(section = "Trade", dataset = "Imports")
  expect_equal(meta$columns$value$description, "Import value in EUR")
  expect_equal(meta$columns$value$units, "EUR (thousands)")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_describe_column works in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "col_lake_test_")
  dir.create(temp_dir)

  db_lake_connect(
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

test_that("db_get_docs returns empty metadata for undocumented dataset", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "get_docs_test_")
  dir.create(temp_dir)
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  meta <- db_get_docs(section = "Trade", dataset = "Imports")

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

test_that("db_dictionary returns empty data.frame when no datasets", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_empty_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  dict <- db_dictionary()

  expect_s3_class(dict, "data.frame")
  expect_equal(nrow(dict), 0)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dictionary includes documented and undocumented datasets", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Create two datasets
  for (ds in c("Imports", "Exports")) {
    path <- file.path(temp_dir, "Trade", ds)
    dir.create(path, recursive = TRUE)

    df <- data.frame(id = 1:3, value = c(100, 200, 300))
    duckdb::duckdb_register(con, "df", df)
    DBI::dbExecute(con, sprintf("COPY df TO '%s' (FORMAT PARQUET)", file.path(path, "data.parquet")))
    duckdb::duckdb_unregister(con, "df")
  }

  # Document only Imports
  db_describe(section = "Trade", dataset = "Imports", description = "Import data", owner = "Trade")

  dict <- db_dictionary(include_columns = FALSE)

  expect_equal(nrow(dict), 2)
  expect_true("Imports" %in% dict$dataset)
  expect_true("Exports" %in% dict$dataset)

  # Imports should have docs, Exports should have NA
  imports_row <- dict[dict$dataset == "Imports", ]
  exports_row <- dict[dict$dataset == "Exports", ]

  expect_equal(imports_row$description, "Import data")
  expect_true(is.na(exports_row$description))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dictionary includes column details when requested", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_cols_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Create dataset with multiple columns
  path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(path, recursive = TRUE)

  df <- data.frame(id = 1:3, value = c(100.0, 200.0, 300.0), country = c("IE", "UK", "DE"))
  duckdb::duckdb_register(con, "df", df)
  DBI::dbExecute(con, sprintf("COPY df TO '%s' (FORMAT PARQUET)", file.path(path, "data.parquet")))
  duckdb::duckdb_unregister(con, "df")

  # Document a column
  db_describe_column(section = "Trade", dataset = "Imports", column = "value", description = "Value in EUR")

  dict <- db_dictionary(include_columns = TRUE)

  expect_true("column_name" %in% names(dict))
  expect_true(nrow(dict) >= 3)  # At least 3 columns

  value_row <- dict[dict$column_name == "value", ]
  expect_equal(value_row$column_description, "Value in EUR")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_dictionary works in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_lake_test_")
  dir.create(temp_dir)

  db_lake_connect(
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

# ==============================================================================
# Tests for db_search()
# ==============================================================================

test_that("db_search finds datasets by name", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Create datasets
  for (ds in c("Imports", "Exports", "Products")) {
    path <- file.path(temp_dir, "Trade", ds)
    dir.create(path, recursive = TRUE)
    df <- data.frame(id = 1)
    duckdb::duckdb_register(con, "df", df)
    DBI::dbExecute(con, sprintf("COPY df TO '%s' (FORMAT PARQUET)", file.path(path, "data.parquet")))
    duckdb::duckdb_unregister(con, "df")
  }

  results <- db_search("port")

  expect_equal(nrow(results), 2)  # Imports and Exports
  expect_true(all(c("Imports", "Exports") %in% results$dataset))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_search finds datasets by description", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_desc_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Create and document datasets
  for (ds in c("Dataset1", "Dataset2")) {
    path <- file.path(temp_dir, "Test", ds)
    dir.create(path, recursive = TRUE)
    df <- data.frame(id = 1)
    duckdb::duckdb_register(con, "df", df)
    DBI::dbExecute(con, sprintf("COPY df TO '%s' (FORMAT PARQUET)", file.path(path, "data.parquet")))
    duckdb::duckdb_unregister(con, "df")
  }

  db_describe(section = "Test", dataset = "Dataset1", description = "Contains trade data")
  db_describe(section = "Test", dataset = "Dataset2", description = "Contains labour data")

  results <- db_search("trade", field = "description")

  expect_equal(nrow(results), 1)
  expect_equal(results$dataset, "Dataset1")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_search finds datasets by tags", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_tags_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Create and tag datasets
  for (ds in c("Dataset1", "Dataset2")) {
    path <- file.path(temp_dir, "Test", ds)
    dir.create(path, recursive = TRUE)
    df <- data.frame(id = 1)
    duckdb::duckdb_register(con, "df", df)
    DBI::dbExecute(con, sprintf("COPY df TO '%s' (FORMAT PARQUET)", file.path(path, "data.parquet")))
    duckdb::duckdb_unregister(con, "df")
  }

  db_describe(section = "Test", dataset = "Dataset1", tags = c("official", "monthly"))
  db_describe(section = "Test", dataset = "Dataset2", tags = c("draft", "annual"))

  results <- db_search("official", field = "tags")

  expect_equal(nrow(results), 1)
  expect_equal(results$dataset, "Dataset1")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_search_columns()
# ==============================================================================

test_that("db_search_columns finds columns across datasets", {
  skip_if_not_installed("arrow")
  clean_db_env()

  temp_dir <- tempfile(pattern = "search_cols_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)
  con <- datapond:::.db_get_con()

  # Create datasets with different columns
  path1 <- file.path(temp_dir, "Test", "Dataset1")
  path2 <- file.path(temp_dir, "Test", "Dataset2")
  dir.create(path1, recursive = TRUE)
  dir.create(path2, recursive = TRUE)

  df1 <- data.frame(id = 1, country_code = "IE")
  df2 <- data.frame(id = 1, region_code = "L")

  duckdb::duckdb_register(con, "df1", df1)
  DBI::dbExecute(con, sprintf("COPY df1 TO '%s' (FORMAT PARQUET)", file.path(path1, "data.parquet")))
  duckdb::duckdb_unregister(con, "df1")

  duckdb::duckdb_register(con, "df2", df2)
  DBI::dbExecute(con, sprintf("COPY df2 TO '%s' (FORMAT PARQUET)", file.path(path2, "data.parquet")))
  duckdb::duckdb_unregister(con, "df2")

  results <- db_search_columns("code")

  expect_equal(nrow(results), 2)
  expect_true("country_code" %in% results$column_name)
  expect_true("region_code" %in% results$column_name)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for Public Catalog Functions (Hive Mode)
# ==============================================================================

test_that("db_set_public errors when not connected", {
  clean_db_env()

  expect_error(db_set_public(section = "Trade", dataset = "Imports"), "Not connected")
})

test_that("db_set_public errors in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_public.ducklake"),
    data_path = temp_dir
  )

  # db_set_public is only available in hive mode
  expect_error(db_set_public(section = "Trade", dataset = "Imports"), "only available in hive mode")

  clean_db_env()
})

test_that("db_set_public publishes metadata to catalog folder", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "public_test_")
  dir.create(temp_dir)

  # Create dataset folder and metadata
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # First create metadata
  db_describe(
    section = "Trade",
    dataset = "Imports",
    description = "Monthly import values",
    owner = "Trade Section"
  )

  # Make public
  expect_message(
    db_set_public(section = "Trade", dataset = "Imports"),
    "Published"
  )

  # Check catalog file exists
  catalog_path <- file.path(temp_dir, "_catalog", "Trade", "Imports.json")
  expect_true(file.exists(catalog_path))

  # Check catalog contents
  catalog_meta <- jsonlite::fromJSON(catalog_path)
  expect_equal(catalog_meta$description, "Monthly import values")
  expect_equal(catalog_meta$section, "Trade")
  expect_equal(catalog_meta$dataset, "Imports")
  expect_true(catalog_meta$public)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_set_public errors when no metadata exists", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "public_nometa_test_")
  dir.create(temp_dir)

  # Create dataset folder without metadata
  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  expect_error(
    db_set_public(section = "Trade", dataset = "Imports"),
    "No metadata exists"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_set_private removes metadata from catalog", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "private_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Create metadata and make public
  db_describe(section = "Trade", dataset = "Imports", description = "Test")
  db_set_public(section = "Trade", dataset = "Imports")

  catalog_path <- file.path(temp_dir, "_catalog", "Trade", "Imports.json")
  expect_true(file.exists(catalog_path))

  # Make private
  expect_message(
    db_set_private(section = "Trade", dataset = "Imports"),
    "Removed"
  )

  expect_false(file.exists(catalog_path))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_is_public returns correct status", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "is_public_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Create metadata
  db_describe(section = "Trade", dataset = "Imports", description = "Test")

  # Initially not public
 expect_false(db_is_public(section = "Trade", dataset = "Imports"))

  # Make public
  db_set_public(section = "Trade", dataset = "Imports")
  expect_true(db_is_public(section = "Trade", dataset = "Imports"))

  # Make private again
  db_set_private(section = "Trade", dataset = "Imports")
  expect_false(db_is_public(section = "Trade", dataset = "Imports"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_list_public returns empty data.frame when no public datasets", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "list_public_empty_test_")
  dir.create(temp_dir)

  db_connect(path = temp_dir)

  result <- db_list_public()

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
  expect_true("section" %in% names(result))
  expect_true("dataset" %in% names(result))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_list_public lists all public datasets", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "list_public_test_")
  dir.create(temp_dir)

  # Create multiple datasets
  for (ds in c("Imports", "Exports")) {
    dir.create(file.path(temp_dir, "Trade", ds), recursive = TRUE)
  }
  dir.create(file.path(temp_dir, "Labour", "Employment"), recursive = TRUE)

  db_connect(path = temp_dir)

  # Document and publish some datasets
  db_describe(section = "Trade", dataset = "Imports", description = "Trade imports", owner = "Trade")
  db_describe(section = "Trade", dataset = "Exports", description = "Trade exports", owner = "Trade")
  db_describe(section = "Labour", dataset = "Employment", description = "Employment data", owner = "Labour")

  db_set_public(section = "Trade", dataset = "Imports")
  db_set_public(section = "Labour", dataset = "Employment")
  # Exports NOT made public

  result <- db_list_public()

  expect_equal(nrow(result), 2)
  expect_true("Imports" %in% result$dataset)
  expect_true("Employment" %in% result$dataset)
  expect_false("Exports" %in% result$dataset)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_list_public filters by section", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "list_public_filter_test_")
  dir.create(temp_dir)

  dir.create(file.path(temp_dir, "Trade", "Imports"), recursive = TRUE)
  dir.create(file.path(temp_dir, "Labour", "Employment"), recursive = TRUE)

  db_connect(path = temp_dir)

  db_describe(section = "Trade", dataset = "Imports", description = "Trade data")
  db_describe(section = "Labour", dataset = "Employment", description = "Labour data")

  db_set_public(section = "Trade", dataset = "Imports")
  db_set_public(section = "Labour", dataset = "Employment")

  result <- db_list_public(section = "Trade")

  expect_equal(nrow(result), 1)
  expect_equal(result$dataset, "Imports")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_describe() with public parameter
# ==============================================================================

test_that("db_describe with public=TRUE publishes to catalog", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "describe_public_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  expect_message(
    db_describe(
      section = "Trade",
      dataset = "Imports",
      description = "Monthly imports",
      public = TRUE
    ),
    "published to catalog"
  )

  expect_true(db_is_public(section = "Trade", dataset = "Imports"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_describe with public=FALSE removes from catalog", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "describe_private_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # First make public
  db_describe(section = "Trade", dataset = "Imports", description = "Test", public = TRUE)
  expect_true(db_is_public(section = "Trade", dataset = "Imports"))

  # Then make private
  expect_message(
    db_describe(section = "Trade", dataset = "Imports", public = FALSE),
    "removed from catalog"
  )

  expect_false(db_is_public(section = "Trade", dataset = "Imports"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_describe auto-syncs when dataset is already public", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "describe_sync_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Make public
  db_describe(section = "Trade", dataset = "Imports", description = "Original", public = TRUE)

  # Update without specifying public parameter
  expect_message(
    db_describe(section = "Trade", dataset = "Imports", description = "Updated"),
    "synced to catalog"
  )

  # Check catalog has updated description
  catalog_path <- file.path(temp_dir, "_catalog", "Trade", "Imports.json")
  catalog_meta <- jsonlite::fromJSON(catalog_path)
  expect_equal(catalog_meta$description, "Updated")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_describe_column() with public parameter
# ==============================================================================

test_that("db_describe_column auto-syncs when dataset is public", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "col_sync_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Make dataset public
  db_describe(section = "Trade", dataset = "Imports", description = "Test", public = TRUE)

  # Add column docs (should auto-sync)
  expect_message(
    db_describe_column(
      section = "Trade",
      dataset = "Imports",
      column = "value",
      description = "Import value"
    ),
    "synced to catalog"
  )

  # Check catalog has column docs
  catalog_path <- file.path(temp_dir, "_catalog", "Trade", "Imports.json")
  catalog_meta <- jsonlite::fromJSON(catalog_path, simplifyVector = FALSE)
  expect_equal(catalog_meta$columns$value$description, "Import value")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_describe_column with public=TRUE errors when dataset not public", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "col_not_public_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Create metadata but don't make public
  db_describe(section = "Trade", dataset = "Imports", description = "Test")

  # Try to add column docs with public=TRUE
  expect_error(
    db_describe_column(
      section = "Trade",
      dataset = "Imports",
      column = "value",
      description = "Test",
      public = TRUE
    ),
    "is not public"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_sync_catalog()
# ==============================================================================

test_that("db_sync_catalog updates all public entries", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "sync_catalog_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Create and publish
  db_describe(section = "Trade", dataset = "Imports", description = "Original", public = TRUE)

  # Manually modify source metadata
  meta_path <- file.path(dataset_path, "_metadata.json")
  meta <- jsonlite::fromJSON(meta_path, simplifyVector = FALSE)
  meta$description <- "Modified"
  jsonlite::write_json(meta, meta_path, pretty = TRUE, auto_unbox = TRUE)

  # Sync catalog
  result <- db_sync_catalog()

  expect_equal(result$synced, 1)
  expect_equal(result$errors, 0)

  # Check catalog is updated
  catalog_path <- file.path(temp_dir, "_catalog", "Trade", "Imports.json")
  catalog_meta <- jsonlite::fromJSON(catalog_path)
  expect_equal(catalog_meta$description, "Modified")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_sync_catalog removes orphans when requested", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "sync_orphan_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Create and publish
  db_describe(section = "Trade", dataset = "Imports", description = "Test", public = TRUE)

  # Manually delete source metadata (simulating deleted dataset)
  unlink(file.path(dataset_path, "_metadata.json"))

  # Sync with remove_orphans = TRUE
  expect_message(
    result <- db_sync_catalog(remove_orphans = TRUE),
    "Removed orphan"
  )

  expect_equal(result$removed, 1)

  # Catalog entry should be gone
  catalog_path <- file.path(temp_dir, "_catalog", "Trade", "Imports.json")
  expect_false(file.exists(catalog_path))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_sync_catalog reports orphans without removing by default", {
  clean_db_env()

  temp_dir <- tempfile(pattern = "sync_report_test_")
  dir.create(temp_dir)

  dataset_path <- file.path(temp_dir, "Trade", "Imports")
  dir.create(dataset_path, recursive = TRUE)

  db_connect(path = temp_dir)

  # Create and publish
  db_describe(section = "Trade", dataset = "Imports", description = "Test", public = TRUE)

  # Delete source metadata
  unlink(file.path(dataset_path, "_metadata.json"))

  # Sync without remove_orphans
  expect_message(
    result <- db_sync_catalog(),
    "Orphan found"
  )

  expect_equal(result$removed, 0)

  # Catalog entry should still exist
  catalog_path <- file.path(temp_dir, "_catalog", "Trade", "Imports.json")
  expect_true(file.exists(catalog_path))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for Public Catalog Functions (DuckLake Mode - now hive-only)
# ==============================================================================

test_that("db_set_public errors in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_no_section.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_set_public(section = "Trade", dataset = "Imports"),
    "only available in hive mode"
  )

  clean_db_env()
})

test_that("db_list_public errors in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()

  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_no_master.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_list_public(), "only available in hive mode")

  clean_db_env()
})

test_that("db_is_public errors in DuckLake mode", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()

  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_is_public.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_is_public(section = "Trade", dataset = "test"),
    "only available in hive mode"
  )

  clean_db_env()
})
