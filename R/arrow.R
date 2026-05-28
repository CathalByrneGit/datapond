# R/arrow.R
# Arrow integration for DuckLake - direct read/write without DuckDB compute

#' Read a DuckLake table as an Arrow Table
#'
#' @description Reads a DuckLake table directly as an Arrow Table, bypassing
#' DuckDB's query engine. This is useful for interoperability with other
#' Arrow-based tools or when you need the raw Arrow format.
#'
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param columns Optional character vector of column names to read.
#'   If NULL (default), reads all columns.
#' @param as_data_frame If TRUE (default), converts to data.frame.
#'   If FALSE, returns an Arrow Table.
#' @return A data.frame or Arrow Table
#' @examples
#' \dontrun{
#' db_connect(...)
#'
#' # Read as data.frame (default)
#' df <- db_read_arrow(table = "imports")
#'
#' # Read as Arrow Table
#' arrow_tbl <- db_read_arrow(table = "imports", as_data_frame = FALSE)
#'
#' # Read specific columns
#' df <- db_read_arrow(table = "imports", columns = c("year", "value"))
#' }
#' @seealso [db_read()] for lazy dplyr-based reading, [db_write_arrow()] for
#'   writing Arrow data
#' @export
db_read_arrow <- function(schema = "main",
                          table,
                          columns = NULL,
                          as_data_frame = TRUE) {

 .db_assert_arrow()

  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")

  con <- .db_get_con()
  if (is.null(con)) {
    stop("Not connected. Use db_connect() first.", call. = FALSE)
  }

  catalog <- .db_get("catalog")
  if (is.null(catalog)) {
    stop("No DuckLake catalog configured. Use db_connect() first.", call. = FALSE)
  }

  # Get the parquet file paths from DuckLake metadata
  files <- .db_get_table_files(con, catalog, schema, table)

  if (length(files) == 0) {
    # Empty table - return empty result with correct schema
    col_info <- db_table_cols(schema = schema, table = table)
    empty_df <- data.frame(matrix(ncol = length(col_info), nrow = 0))
    names(empty_df) <- col_info
    if (as_data_frame) return(empty_df)
    return(arrow::as_arrow_table(empty_df))
  }

  # Read parquet files via Arrow
  if (length(files) == 1) {
    tbl <- arrow::read_parquet(files[1], col_select = columns)
  } else {
    # Multiple files - use Arrow Dataset
    ds <- arrow::open_dataset(files)
    if (!is.null(columns)) {
      ds <- ds |> dplyr::select(dplyr::all_of(columns))
    }
    tbl <- ds |> dplyr::collect()
    if (!as_data_frame) {
      tbl <- arrow::as_arrow_table(tbl)
    }
    return(tbl)
  }

  if (as_data_frame) {
    as.data.frame(tbl)
  } else {
    arrow::as_arrow_table(tbl)
  }
}


#' Write an Arrow Table to DuckLake
#'
#' @description Writes an Arrow Table or RecordBatch directly to DuckLake.
#' This provides an alternative to `db_write()` when your data is already
#' in Arrow format.
#'
#' @param data An Arrow Table, RecordBatch, or data.frame
#' @param schema Schema name (default "main")
#' @param table Table name
#' @param mode "overwrite" or "append"
#' @param commit_author Optional author for DuckLake commit metadata
#' @param commit_message Optional message for DuckLake commit metadata
#' @return Invisibly returns the qualified table name
#' @examples
#' \dontrun{
#' db_connect(...)
#'
#' # Write Arrow Table
#' arrow_tbl <- arrow::arrow_table(id = 1:3, value = c(10, 20, 30))
#' db_write_arrow(arrow_tbl, table = "metrics")
#'
#' # Write from parquet file
#' arrow_tbl <- arrow::read_parquet("data.parquet")
#' db_write_arrow(arrow_tbl, table = "imports", mode = "append")
#' }
#' @seealso [db_write()] for writing data.frames with more options,
#'   [db_read_arrow()] for reading as Arrow
#' @export
db_write_arrow <- function(data,
                           schema = "main",
                           table,
                           mode = c("overwrite", "append"),
                           commit_author = NULL,
                           commit_message = NULL) {

  .db_assert_arrow()

  mode <- match.arg(mode)
  schema <- .db_validate_name(schema, "schema")
  table  <- .db_validate_name(table, "table")

 # Convert to data.frame for DuckDB registration
  if (inherits(data, c("ArrowTabular", "RecordBatch", "Table", "arrow_dplyr_query"))) {
    data <- as.data.frame(data)
  }

  if (!is.data.frame(data)) {
    stop("data must be an Arrow Table, RecordBatch, or data.frame.", call. = FALSE)
  }

  # Delegate to db_write
  db_write(
    data = data,
    schema = schema,
    table = table,
    mode = mode,
    commit_author = commit_author,
    commit_message = commit_message
  )
}


#' Get Parquet file paths for a DuckLake table
#' @noRd
.db_get_table_files <- function(con, catalog, schema, table) {
  # Use ducklake_table_data_files function instead of querying metadata directly
  sql <- glue::glue("
    SELECT path
    FROM ducklake_table_data_files({.db_sql_quote(catalog)}, {.db_sql_quote(table)}, schema_name => {.db_sql_quote(schema)})
    WHERE path IS NOT NULL
  ")

  result <- tryCatch({
    DBI::dbGetQuery(con, sql)
  }, error = function(e) {
    data.frame(path = character(0))
  })

  # Resolve relative paths to absolute using data_path
  data_path <- .db_get("data_path")
  paths <- result$path

  if (length(paths) > 0 && !is.null(data_path)) {
    # Check if paths are relative
    paths <- sapply(paths, function(p) {
      if (!grepl("^(/|[A-Za-z]:)", p)) {
        file.path(data_path, p)
      } else {
        p
      }
    }, USE.NAMES = FALSE)
  }

  paths
}


#' Assert that Arrow package is available
#' @noRd
.db_assert_arrow <- function() {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop(
      "The 'arrow' package is required for this function.\n",
      "Install with: install.packages('arrow')",
      call. = FALSE
    )
  }
  invisible(TRUE)
}
