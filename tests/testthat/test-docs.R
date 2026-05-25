# tests/testthat/test-docs.R

# ==============================================================================
# Tests for db_comment() with structured metadata
# ==============================================================================

test_that("db_comment stores and retrieves table metadata", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "comment_table_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create table
  db_write(data.frame(id = 1:3), schema = "main", table = "products")

  # Add structured metadata
  expect_message(
    db_comment(
      table = "products",
      comment = list(
        description = "Product catalog",
        owner = "Sales Team",
        tags = c("products", "reference")
      )
    ),
    "Set comment"
  )

  # Retrieve and check
  meta <- db_get_docs(table = "products")
  expect_equal(meta$description, "Product catalog")
  expect_equal(meta$owner, "Sales Team")
  expect_equal(meta$tags, c("products", "reference"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_comment stores and retrieves column metadata", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "comment_col_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  # Create table
  db_write(data.frame(id = 1:3, price = c(10.5, 20.5, 30.5)),
           schema = "main", table = "products")

  # Add column metadata
  db_comment(
    table = "products",
    column = "price",
    comment = list(
      description = "Product price",
      units = "EUR"
    )
  )

  meta <- db_get_docs(table = "products")
  expect_equal(meta$columns$price$description, "Product price")
  expect_equal(meta$columns$price$units, "EUR")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_comment works with simple string", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "comment_string_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3), schema = "main", table = "simple")

  # Simple string comment
  db_comment(table = "simple", comment = "Just a plain comment")

  meta <- db_get_docs(table = "simple")
  expect_equal(meta$description, "Just a plain comment")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_comment metadata survives disconnect/reconnect", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "comment_persist_test_")
  dir.create(temp_dir)

  # First connection - add metadata
  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3), schema = "main", table = "persist_test")

  db_comment(table = "persist_test", comment = list(
    description = "Should persist",
    owner = "Test Owner"
  ))

  db_disconnect()

  # Second connection - verify metadata persists
  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  meta <- db_get_docs(table = "persist_test")
  expect_equal(meta$description, "Should persist")
  expect_equal(meta$owner, "Test Owner")

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

  db_write(data.frame(id = 1:3), schema = "main", table = "no_docs")

  meta <- db_get_docs(table = "no_docs")

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

test_that("db_dictionary works with documented tables", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "dict_lake_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3, name = letters[1:3]),
           schema = "main", table = "products")
  db_write(data.frame(id = 1:3, total = c(100, 200, 300)),
           schema = "main", table = "orders")

  db_comment(table = "products", comment = list(description = "Product list"))

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

  temp_dir <- tempfile(pattern = "dict_cols_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3, name = letters[1:3], price = c(10.5, 20.5, 30.5)),
           schema = "main", table = "products")

  # Document a column
  db_comment(table = "products", column = "price",
             comment = list(description = "Price in EUR"))

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

  db_write(data.frame(id = 1), schema = "main", table = "imports")
  db_write(data.frame(id = 1), schema = "main", table = "exports")
  db_write(data.frame(id = 1), schema = "main", table = "products")

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

  db_write(data.frame(id = 1), schema = "main", table = "table1")
  db_write(data.frame(id = 1), schema = "main", table = "table2")

  db_comment(table = "table1", comment = list(description = "Contains trade data"))
  db_comment(table = "table2", comment = list(description = "Contains labour data"))

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

  db_write(data.frame(id = 1), schema = "main", table = "table1")
  db_write(data.frame(id = 1), schema = "main", table = "table2")

  db_comment(table = "table1", comment = list(tags = c("official", "monthly")))
  db_comment(table = "table2", comment = list(tags = c("draft", "annual")))

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

  db_write(data.frame(id = 1, country_code = "IE"),
           schema = "main", table = "table1")
  db_write(data.frame(id = 1, region_code = "EU"),
           schema = "main", table = "table2")

  results <- db_search_columns("code")

  expect_equal(nrow(results), 2)
  expect_true("country_code" %in% results$column_name)
  expect_true("region_code" %in% results$column_name)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_lineage()
# ==============================================================================

test_that("db_lineage errors when not connected", {
  clean_db_env()

  expect_error(db_lineage(table = "test", sources = "raw.data"), "Not connected")
})

test_that("db_lineage records lineage for a table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lineage_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3, value = c(10, 20, 30)),
           schema = "main", table = "summary")

  expect_message(
    db_lineage(
      table = "summary",
      sources = c("raw.transactions", "raw.products"),
      transformation = "Aggregated by month"
    ),
    "Recorded lineage"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lineage errors for non-existent table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lineage_noexist_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  expect_error(
    db_lineage(table = "nonexistent", sources = "raw.data"),
    "not found"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_lineage preserves existing table metadata", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "lineage_preserve_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3), schema = "main", table = "with_meta")

  # Add description first
  db_comment(table = "with_meta", comment = list(
    description = "Test table",
    owner = "Test Owner"
  ))

  # Add lineage
  db_lineage(table = "with_meta", sources = c("source1", "source2"))

  # Check both are preserved
  meta <- db_get_docs(table = "with_meta")
  expect_equal(meta$description, "Test table")
  expect_equal(meta$owner, "Test Owner")
  expect_equal(meta$lineage$sources, c("source1", "source2"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})


# ==============================================================================
# Tests for db_get_lineage()
# ==============================================================================

test_that("db_get_lineage errors when not connected", {
  clean_db_env()

  expect_error(db_get_lineage(table = "test"), "Not connected")
})

test_that("db_get_lineage returns NULL for table without lineage", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "get_lineage_null_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3), schema = "main", table = "no_lineage")

  result <- db_get_lineage(table = "no_lineage")
  expect_null(result)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_get_lineage retrieves recorded lineage", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "get_lineage_test_")
  dir.create(temp_dir)

  db_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  db_write(data.frame(id = 1:3), schema = "main", table = "with_lineage")

  db_lineage(
    table = "with_lineage",
    sources = c("source_a", "source_b"),
    transformation = "Joined and filtered"
  )

  result <- db_get_lineage(table = "with_lineage")

  expect_type(result, "list")
  expect_equal(result$sources, c("source_a", "source_b"))
  expect_equal(result$transformation, "Joined and filtered")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
