---
name: Feature tracking
about: SQL Server catalog backend support
title: "Add SQL Server as catalog backend (tracking DuckLake support)"
labels: enhancement, blocked
---

## Feature Request

Add support for Microsoft SQL Server as a DuckLake catalog backend.

## Current Status: Blocked

This feature is **blocked** waiting for upstream DuckLake support.

- **Upstream tracking**: https://github.com/duckdb/ducklake/discussions/892
- **DuckDB SQL Server extension**: https://github.com/hugr-lab/mssql-extension (exists, works)
- **DuckLake SQL Server catalog**: Not yet implemented

## Why This Matters

Many enterprise environments use SQL Server as their primary database. Requiring PostgreSQL or SQLite for DuckLake catalog when SQL Server is already available creates unnecessary infrastructure complexity.

## What's Needed

### Upstream (DuckLake)
1. DuckLake needs to add `ducklake:mssql:` DSN support
2. DuckLake needs to generate SQL Server-compatible DDL for metadata tables

### datapond (trivial once upstream supports it)

```r
# R/db_connect.R - add to .db_build_ducklake_dsn()
mssql = paste0("ducklake:mssql:", metadata_path)

# R/db_connect.R - add to .db_detect_catalog_type()
# Detection for mssql:// or Server= connection strings

# R/db_connect.R - add to .db_load_catalog_extensions()
} else if (catalog_type == "mssql") {
  try(DBI::dbExecute(con, "INSTALL mssql FROM community"), silent = TRUE)
  DBI::dbExecute(con, "LOAD mssql")
}
```

## Workarounds

Until DuckLake adds SQL Server support:

1. **PostgreSQL** - Use Azure Database for PostgreSQL in Azure environments
2. **SQLite** - Use SQLite file on shared storage
3. **DuckDB** - Use DuckDB file for single-user scenarios

## References

- DuckLake catalog backends: https://ducklake.select/docs/stable/duckdb/usage/choosing_storage
- hugr-lab mssql extension: https://github.com/hugr-lab/mssql-extension
- DuckLake 1.0 release: https://ducklake.select/2026/04/13/ducklake-10/
