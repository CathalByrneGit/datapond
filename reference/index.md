# Package index

## All functions

- [`db_browser()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser.md)
  : Browse the data lake interactively
- [`db_browser_server()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_server.md)
  : Shiny module server for db_browser()
- [`db_browser_ui()`](https://cathalbyrnegit.github.io/datapond/reference/db_browser_ui.md)
  : Shiny module UI for db_browser()
- [`db_catalog()`](https://cathalbyrnegit.github.io/datapond/reference/db_catalog.md)
  : List tables and file stats tracked by DuckLake
- [`db_changes()`](https://cathalbyrnegit.github.io/datapond/reference/db_changes.md)
  : Get row-level changes from the Data Change Feed
- [`db_cleanup_files()`](https://cathalbyrnegit.github.io/datapond/reference/db_cleanup_files.md)
  : Clean up orphaned files from DuckLake storage
- [`db_comment()`](https://cathalbyrnegit.github.io/datapond/reference/db_comment.md)
  : Add comment/metadata to table or column
- [`db_compact()`](https://cathalbyrnegit.github.io/datapond/reference/db_compact.md)
  : Compact small files in a DuckLake table
- [`db_connect()`](https://cathalbyrnegit.github.io/datapond/reference/db_connect.md)
  : Connect to a DuckLake data lake
- [`db_create_macro()`](https://cathalbyrnegit.github.io/datapond/reference/db_create_macro.md)
  : Create a macro in DuckLake
- [`db_create_schema()`](https://cathalbyrnegit.github.io/datapond/reference/db_create_schema.md)
  : Create a new schema in DuckLake
- [`db_create_view()`](https://cathalbyrnegit.github.io/datapond/reference/db_create_view.md)
  : Create a view in DuckLake
- [`db_deletions()`](https://cathalbyrnegit.github.io/datapond/reference/db_deletions.md)
  : Get deleted rows from the Data Change Feed
- [`db_dictionary()`](https://cathalbyrnegit.github.io/datapond/reference/db_dictionary.md)
  : Generate a data dictionary
- [`db_diff()`](https://cathalbyrnegit.github.io/datapond/reference/db_diff.md)
  : Compare a table between two snapshots
- [`db_disconnect()`](https://cathalbyrnegit.github.io/datapond/reference/db_disconnect.md)
  : Disconnect from the CSO Data Lake
- [`db_drop_macro()`](https://cathalbyrnegit.github.io/datapond/reference/db_drop_macro.md)
  : Drop a macro from DuckLake
- [`db_drop_view()`](https://cathalbyrnegit.github.io/datapond/reference/db_drop_view.md)
  : Drop a view from DuckLake
- [`db_enable_logging()`](https://cathalbyrnegit.github.io/datapond/reference/db_enable_logging.md)
  : Enable DuckDB/DuckLake logging
- [`db_export_iceberg()`](https://cathalbyrnegit.github.io/datapond/reference/db_export_iceberg.md)
  : Export a DuckLake table as Iceberg format (EXPERIMENTAL)
- [`db_file_stats()`](https://cathalbyrnegit.github.io/datapond/reference/db_file_stats.md)
  : Get file statistics for DuckLake tables
- [`db_flush_inlined()`](https://cathalbyrnegit.github.io/datapond/reference/db_flush_inlined.md)
  : Flush inlined data to parquet files
- [`db_get_docs()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_docs.md)
  : Get documentation for a table
- [`db_get_lineage()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_lineage.md)
  : Get lineage information
- [`db_get_partitioning()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_partitioning.md)
  : Get partitioning configuration for a DuckLake table
- [`db_iceberg_metadata()`](https://cathalbyrnegit.github.io/datapond/reference/db_iceberg_metadata.md)
  : Get Iceberg metadata for a DuckLake table
- [`db_insertions()`](https://cathalbyrnegit.github.io/datapond/reference/db_insertions.md)
  : Get inserted rows from the Data Change Feed
- [`db_lineage()`](https://cathalbyrnegit.github.io/datapond/reference/db_lineage.md)
  : Record data lineage
- [`db_list_macros()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_macros.md)
  : List macros in a DuckLake schema
- [`db_list_schemas()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_schemas.md)
  : List schemas in the DuckLake catalog
- [`db_list_views()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_views.md)
  : List views in a DuckLake schema
- [`db_preview_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_upsert.md)
  : Preview an upsert operation
- [`db_preview_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_write.md)
  : Preview a DuckLake write operation
- [`db_query()`](https://cathalbyrnegit.github.io/datapond/reference/db_query.md)
  : Run arbitrary SQL and return results
- [`db_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_read.md)
  : Read a DuckLake table (lazy)
- [`db_read_arrow()`](https://cathalbyrnegit.github.io/datapond/reference/db_read_arrow.md)
  : Read a DuckLake table as an Arrow Table
- [`db_recluster()`](https://cathalbyrnegit.github.io/datapond/reference/db_recluster.md)
  : Re-cluster table data
- [`db_rollback()`](https://cathalbyrnegit.github.io/datapond/reference/db_rollback.md)
  : Rollback a table to a previous snapshot
- [`db_search()`](https://cathalbyrnegit.github.io/datapond/reference/db_search.md)
  : Search for tables
- [`db_search_columns()`](https://cathalbyrnegit.github.io/datapond/reference/db_search_columns.md)
  : Search for columns by name
- [`db_set_clustering()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_clustering.md)
  : Set sort order for a table
- [`db_set_inline_threshold()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_inline_threshold.md)
  : Set the inline threshold for a table, schema, or globally
- [`db_set_partitioning()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_partitioning.md)
  : Set partitioning for a DuckLake table
- [`db_snapshots()`](https://cathalbyrnegit.github.io/datapond/reference/db_snapshots.md)
  : List DuckLake snapshots
- [`db_status()`](https://cathalbyrnegit.github.io/datapond/reference/db_status.md)
  : Get connection status and configuration
- [`db_table_cols()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_cols.md)
  : Get column names for a DuckLake table
- [`db_table_exists()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_exists.md)
  : Check if a DuckLake table exists
- [`db_tables()`](https://cathalbyrnegit.github.io/datapond/reference/db_tables.md)
  : List tables in a DuckLake schema
- [`db_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_upsert.md)
  : Upsert into a DuckLake table using MERGE INTO
- [`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md)
  : Vacuum old snapshots from DuckLake
- [`db_view_cols()`](https://cathalbyrnegit.github.io/datapond/reference/db_view_cols.md)
  : Get column names for a DuckLake view
- [`db_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_write.md)
  : Write a DuckLake table (overwrite/append)
- [`db_write_arrow()`](https://cathalbyrnegit.github.io/datapond/reference/db_write_arrow.md)
  : Write an Arrow Table to DuckLake
- [`run_example()`](https://cathalbyrnegit.github.io/datapond/reference/run_example.md)
  : Run a package example script
