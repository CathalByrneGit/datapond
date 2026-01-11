# tests/testthat/test-browser.R

# ==============================================================================
# Tests for db_browser() and related functions
# ==============================================================================

test_that("db_browser errors when not connected", {
  skip_if_not_installed("shiny")
  skip_if_not_installed("bslib")
  skip_if_not_installed("DT")

  clean_db_env()

  expect_error(db_browser(), "Not connected")
})

test_that(".db_assert_browser_packages checks for required packages", {
  # This will pass since we're requiring the packages in the skip_if_not_installed above
  skip_if_not_installed("shiny")
  skip_if_not_installed("bslib")
  skip_if_not_installed("DT")

  expect_true(datapond:::.db_assert_browser_packages())
})

test_that("db_browser_ui creates UI without error in hive mode", {
  skip_if_not_installed("shiny")
  skip_if_not_installed("bslib")
  skip_if_not_installed("DT")

  clean_db_env()
  db_connect(path = tempdir())

  ui <- db_browser_ui("test_id")


  # Check it returns a bslib page (which is a shiny.tag.list)
  expect_true(inherits(ui, "shiny.tag.list") || inherits(ui, "shiny.tag"))

  clean_db_env()
})

test_that("db_browser_ui creates UI without error in DuckLake mode", {
  skip_if_not_installed("shiny")
  skip_if_not_installed("bslib")
  skip_if_not_installed("DT")
  skip_if_not(ducklake_available(), "DuckLake extension not available")

  clean_db_env()

  temp_dir <- tempfile(pattern = "browser_lake_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  ui <- db_browser_ui("test_id")

  # Check it returns a bslib page (which is a shiny.tag.list)
  expect_true(inherits(ui, "shiny.tag.list") || inherits(ui, "shiny.tag"))

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that(".render_hive_tree returns valid HTML", {
  skip_if_not_installed("shiny")

  clean_db_env()

  temp_dir <- tempfile(pattern = "tree_test_")
  dir.create(temp_dir)

  # Create some sections and datasets
  dir.create(file.path(temp_dir, "Trade", "Imports"), recursive = TRUE)
  dir.create(file.path(temp_dir, "Trade", "Exports"), recursive = TRUE)
  dir.create(file.path(temp_dir, "Labour", "Employment"), recursive = TRUE)

  db_connect(path = temp_dir)

  ns <- shiny::NS("test")
  rv <- shiny::reactiveValues()

  # Should not error
  result <- datapond:::.render_hive_tree(ns, rv)
  expect_s3_class(result, "shiny.tag")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that(".render_ducklake_tree returns valid HTML", {
  skip_if_not_installed("shiny")
  skip_if_not(ducklake_available(), "DuckLake extension not available")

  clean_db_env()

  temp_dir <- tempfile(pattern = "tree_lake_")
  dir.create(temp_dir)

  db_lake_connect(
    catalog = "test",
    metadata_path = file.path(temp_dir, "catalog.ducklake"),
    data_path = temp_dir
  )

  con <- datapond:::.db_get_con()
  DBI::dbExecute(con, "CREATE TABLE test.main.products (id INTEGER)")

  ns <- shiny::NS("test")
  rv <- shiny::reactiveValues()

  result <- datapond:::.render_ducklake_tree(ns, rv)
  expect_s3_class(result, "shiny.tag")

  clean_db_env()
  unlink(temp_dir, recursive = TRUE)
})

test_that(".render_metadata_card returns valid HTML", {
  skip_if_not_installed("shiny")

  ns <- shiny::NS("test")
  rv <- list(
    selected_section = "Trade",
    selected_dataset = "Imports"
  )

  meta <- list(
    description = "Test description",
    owner = "Test owner",
    tags = c("tag1", "tag2"),
    columns = list(
      id = list(description = "ID column"),
      value = list(description = "Value", units = "EUR")
    )
  )

  result <- datapond:::.render_metadata_card(meta, is_hive = TRUE, rv = rv, ns = ns)
  expect_s3_class(result, "shiny.tag")
})
