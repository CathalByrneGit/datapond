# R/zzz.R

# dbplyr is required for dplyr::tbl() with database connections
# This import ensures it's loaded and satisfies R CMD check
#' @import dbplyr
NULL

# Null coalesce operator (handles NULL and length-0)
# Used throughout the package
#' @noRd
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

.onLoad <- function(libname, pkgname) {
  # Nothing needed on load currently
  # Could add:
  # - Default options via options()
  # - Package-level configuration
  invisible()
}

.onUnload <- function(libpath) {
  # Clean up any active connection when package is unloaded
  con <- .db_get_con()
  if (!is.null(con)) {
    try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  }

  # Clear the environment
  if (exists(".db_env", envir = asNamespace("datapond"), inherits = FALSE)) {
    rm(list = ls(envir = .db_env), envir = .db_env)
  }

  invisible()
}

.onAttach <- function(libname, pkgname) {

  packageStartupMessage(
    "datapond ", utils::packageVersion("datapond"), "\n",
    "Use db_connect() for hive mode or db_lake_connect() for DuckLake mode."
  )
}
