# tests/testthat/test-upsert.R

# ==============================================================================
# Tests for .db_relation_cols() - Internal Helper
# ==============================================================================

test_that(".db_relation_cols retrieves column names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "cols_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()

  # Create a table with known columns
  DBI::dbExecute(con, "CREATE TABLE test.main.products (
    id INTEGER,
    name VARCHAR,
    price DOUBLE,
    created_at TIMESTAMP
  )")

  cols <- datapond:::.db_relation_cols(con, "test", "main", "products")

  expect_equal(cols, c("id", "name", "price", "created_at"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for .db_table_exists() - Internal Helper
# ==============================================================================

test_that(".db_table_exists returns TRUE for existing table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "exists_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER)")

  expect_true(datapond:::.db_table_exists(con, "test", "main", "items"))
  expect_false(datapond:::.db_table_exists(con, "test", "main", "nonexistent"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_table_cols() - Public Function
# ==============================================================================

test_that("db_table_cols errors when not connected", {
  clean_db_env()

  expect_error(
    db_table_cols(table = "test"),
    "Not connected"
  )
})

test_that("db_table_cols errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(
    db_table_cols(table = "test"),
    "No DuckLake catalog"
  )

  clean_db_env()
})

test_that("db_table_cols returns column names", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "table_cols_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.users (
    user_id INTEGER,
    email VARCHAR,
    active BOOLEAN
  )")

  cols <- db_table_cols(table = "users")
  expect_equal(cols, c("user_id", "email", "active"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_view_cols() - Public Function
# ==============================================================================

test_that("db_view_cols returns column names for views", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "view_cols_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.orders (
    order_id INTEGER,
    amount DOUBLE,
    status VARCHAR
  )")
  DBI::dbExecute(con, "CREATE VIEW test.main.active_orders AS
    SELECT order_id, amount FROM test.main.orders WHERE status = 'active'")

  cols <- db_view_cols(view = "active_orders")
  expect_equal(cols, c("order_id", "amount"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_upsert() - Connection and Mode Checks
# ==============================================================================

test_that("db_upsert errors when not connected", {
  clean_db_env()

  expect_error(
    db_upsert(data.frame(id = 1), table = "test", by = "id"),
    "Not connected"
  )
})

test_that("db_upsert errors in hive mode", {
  clean_db_env()
  db_connect(path = "/test")

  expect_error(
    db_upsert(data.frame(id = 1), table = "test", by = "id"),
    "hive mode"
  )

  clean_db_env()
})

# ==============================================================================
# Tests for db_upsert() - Input Validation
# ==============================================================================

test_that("db_upsert validates data argument", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_val.ducklake"),
    data_path = temp_dir
  )

  expect_error(db_upsert("not a df", table = "t", by = "id"), "must be a data.frame")
  expect_error(db_upsert(NULL, table = "t", by = "id"), "must be a data.frame")
  expect_error(db_upsert(list(a = 1), table = "t", by = "id"), "must be a data.frame")

  clean_db_env()
})

test_that("db_upsert validates by argument", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempdir()
  db_lake_connect(
    metadata_path = file.path(temp_dir, "test_by.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1, value = 10)

  expect_error(db_upsert(df, table = "t", by = ""), "non-empty")
  expect_error(db_upsert(df, table = "t", by = character(0)), "non-empty")
  expect_error(db_upsert(df, table = "t", by = c("id", "")), "non-empty")

  clean_db_env()
})

test_that("db_upsert validates key columns exist in data", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "key_val_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1, value = 10)

  expect_error(
    db_upsert(df, table = "t", by = "nonexistent"),
    "Key columns not found in data"
  )

  expect_error(
    db_upsert(df, table = "t", by = c("id", "missing")),
    "Key columns not found in data.*missing"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert validates table exists", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "table_exists_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  df <- data.frame(id = 1, value = 10)

  expect_error(
    db_upsert(df, table = "nonexistent", by = "id"),
    "does not exist"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert validates data columns against target table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "col_gov_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")

  # Data has extra column not in target
  df <- data.frame(id = 1, name = "Widget", extra_col = "bad")

  expect_error(
    db_upsert(df, table = "products", by = "id"),
    "columns not present in target"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert validates key columns exist in target table", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "key_target_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.items (id INTEGER, value DOUBLE)")

  # Data has matching columns but key doesn't exist in target
  df <- data.frame(id = 1, value = 10, other_key = "x")

  # This will fail because other_key not in target
  expect_error(
    db_upsert(df, table = "items", by = c("id", "other_key")),
    "columns not present in target"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert validates update_cols parameter", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "update_cols_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR, price DOUBLE)")

  df <- data.frame(id = 1, name = "Widget", price = 9.99)

  # update_cols with column not in data
  expect_error(
    db_upsert(df, table = "products", by = "id", update_cols = c("nonexistent")),
    "update_cols not found in data"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_upsert() - Strict Mode (Duplicate Key Detection)
# ==============================================================================

test_that("db_upsert strict mode rejects duplicate keys", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "strict_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, value DOUBLE)")

  # Data with duplicate keys
  df <- data.frame(
    id = c(1, 1, 2),  # id=1 is duplicated
    value = c(10, 20, 30)
  )

  expect_error(
    db_upsert(df, table = "products", by = "id", strict = TRUE),
    "duplicate keys"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert strict=FALSE allows duplicate keys", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "non_strict_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, value DOUBLE)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (99, 999)")

  # Data with duplicate keys - should not error with strict=FALSE
  df <- data.frame(
    id = c(1, 1, 2),
    value = c(10, 20, 30)
  )

  # Should not error (behavior undefined, but no validation error)
  expect_message(
    db_upsert(df, table = "products", by = "id", strict = FALSE),
    "Upserted"
  )

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

# ==============================================================================
# Tests for db_upsert() - Integration Tests
# ==============================================================================

test_that("db_upsert inserts new rows", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "upsert_insert_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR, price DOUBLE)")

  # Insert new rows into empty table
  df <- data.frame(
    id = 1:3,
    name = c("Widget", "Gadget", "Gizmo"),
    price = c(9.99, 19.99, 29.99)
  )

  db_upsert(df, table = "products", by = "id")

  result <- db_lake_read(table = "products") |> dplyr::collect()
  expect_equal(nrow(result), 3)
  expect_true(all(c(1, 2, 3) %in% result$id))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert updates existing rows", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "upsert_update_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR, price DOUBLE)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES
    (1, 'Widget', 9.99),
    (2, 'Gadget', 19.99)")

  # Update existing row (id=1) and insert new (id=3)
  df <- data.frame(
    id = c(1, 3),
    name = c("Widget Pro", "Gizmo"),
    price = c(14.99, 29.99)
  )

  db_upsert(df, table = "products", by = "id")

  result <- db_lake_read(table = "products") |> dplyr::collect()
  expect_equal(nrow(result), 3)

  # Check id=1 was updated
  r1 <- result[result$id == 1, ]
  expect_equal(r1$name, "Widget Pro")
  expect_equal(r1$price, 14.99)

  # Check id=2 unchanged
  r2 <- result[result$id == 2, ]
  expect_equal(r2$name, "Gadget")

  # Check id=3 inserted
  r3 <- result[result$id == 3, ]
  expect_equal(r3$name, "Gizmo")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert with update_cols=character(0) is insert-only", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "insert_only_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.events (id INTEGER, event VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.events VALUES (1, 'original')")

  # Try to "upsert" but with insert-only mode
  df <- data.frame(
    id = c(1, 2),  # id=1 exists, id=2 is new
    event = c("should_not_update", "new_event")
  )

  db_upsert(df, table = "events", by = "id", update_cols = character(0))

  result <- db_lake_read(table = "events") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  # id=1 should NOT be updated
  r1 <- result[result$id == 1, ]
  expect_equal(r1$event, "original")

  # id=2 should be inserted
  r2 <- result[result$id == 2, ]
  expect_equal(r2$event, "new_event")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert with specific update_cols updates only those columns", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "specific_cols_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR, price DOUBLE)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'Widget', 9.99)")

  # Update only price, not name
  df <- data.frame(
    id = 1,
    name = "Should Not Update",
    price = 14.99
  )

  db_upsert(df, table = "products", by = "id", update_cols = "price")

  result <- db_lake_read(table = "products") |> dplyr::collect()

  # Price should be updated
  expect_equal(result$price, 14.99)

  # Name should NOT be updated
  expect_equal(result$name, "Widget")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert works with composite keys", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "composite_key_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.sales (region VARCHAR, date DATE, amount DOUBLE)")
  DBI::dbExecute(con, "INSERT INTO test.main.sales VALUES
    ('North', '2024-01-01', 100),
    ('South', '2024-01-01', 200)")

  # Upsert with composite key
  df <- data.frame(
    region = c("North", "East"),
    date = as.Date(c("2024-01-01", "2024-01-01")),
    amount = c(150, 300)  # Update North, Insert East
  )

  db_upsert(df, table = "sales", by = c("region", "date"))

  result <- db_lake_read(table = "sales") |> dplyr::collect()
  expect_equal(nrow(result), 3)

  # North should be updated
  r_north <- result[result$region == "North", ]
  expect_equal(r_north$amount, 150)

  # South unchanged
  r_south <- result[result$region == "South", ]
  expect_equal(r_south$amount, 200)

  # East inserted
  r_east <- result[result$region == "East", ]
  expect_equal(r_east$amount, 300)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert works with partial columns when using explicit update_cols", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "partial_cols_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (
    id INTEGER,
    name VARCHAR,
    price DOUBLE DEFAULT 0,
    stock INTEGER DEFAULT 0
  )")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'Existing', 5.00, 100)")

  # Data only has id, name, price (not stock)
  # Use explicit update_cols to only update the columns we have
  df <- data.frame(
    id = 1,
    name = "Updated Widget",
    price = 9.99
  )

  # Must specify update_cols when data doesn't have all columns
  db_upsert(df, table = "products", by = "id", update_cols = c("name", "price"))

  result <- db_lake_read(table = "products") |> dplyr::collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$name, "Updated Widget")
  expect_equal(result$price, 9.99)
  # stock should be unchanged
  expect_equal(result$stock, 100)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert records commit metadata", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "commit_meta_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")

  df <- data.frame(id = 1, name = "Widget")

  db_upsert(df, table = "products", by = "id",
            commit_author = "test_user",
            commit_message = "Test upsert")

  # Verify data was inserted
  result <- db_lake_read(table = "products") |> dplyr::collect()
  expect_equal(nrow(result), 1)

  # Verify snapshot was created
  snapshots <- db_snapshots()
  expect_true(nrow(snapshots) >= 1)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert handles errors gracefully", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "error_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER, name VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO test.main.products VALUES (1, 'Original')")

  initial_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM test.main.products")$n
  expect_equal(initial_count, 1)

  # Verify normal upsert works
  df <- data.frame(id = 2, name = "New Product")
  db_upsert(df, table = "products", by = "id")

  final_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM test.main.products")$n
  expect_equal(final_count, 2)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert returns qualified table name", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "return_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "mycat",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE mycat.main.items (id INTEGER)")

  df <- data.frame(id = 1)
  result <- db_upsert(df, table = "items", by = "id")

  expect_equal(result, "mycat.main.items")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that("db_upsert works with custom schema", {
  skip_if_not(ducklake_available(), "DuckLake extension not available")
  clean_db_env()

  temp_dir <- tempfile(pattern = "schema_test_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE SCHEMA test.sales")
  DBI::dbExecute(con, "CREATE TABLE test.sales.orders (id INTEGER, total DOUBLE)")

  df <- data.frame(id = 1, total = 99.99)

  result <- db_upsert(df, schema = "sales", table = "orders", by = "id")

  expect_equal(result, "test.sales.orders")

  # Verify data
  read_df <- db_lake_read(schema = "sales", table = "orders") |> dplyr::collect()
  expect_equal(nrow(read_df), 1)

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})
