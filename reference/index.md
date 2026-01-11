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
- [`db_connect()`](https://cathalbyrnegit.github.io/datapond/reference/db_connect.md)
  : Connect to the CSO hive parquet lake
- [`db_create_schema()`](https://cathalbyrnegit.github.io/datapond/reference/db_create_schema.md)
  : Create a new schema in DuckLake
- [`db_dataset_exists()`](https://cathalbyrnegit.github.io/datapond/reference/db_dataset_exists.md)
  : Check if a hive dataset exists
- [`db_describe()`](https://cathalbyrnegit.github.io/datapond/reference/db_describe.md)
  : Describe a dataset or table
- [`db_describe_column()`](https://cathalbyrnegit.github.io/datapond/reference/db_describe_column.md)
  : Describe a column
- [`db_dictionary()`](https://cathalbyrnegit.github.io/datapond/reference/db_dictionary.md)
  : Generate a data dictionary
- [`db_diff()`](https://cathalbyrnegit.github.io/datapond/reference/db_diff.md)
  : Compare a table between two snapshots
- [`db_disconnect()`](https://cathalbyrnegit.github.io/datapond/reference/db_disconnect.md)
  : Disconnect from the CSO Data Lake
- [`db_get_docs()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_docs.md)
  : Get documentation for a dataset or table
- [`db_get_schema_path()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_schema_path.md)
  : Get the data path for a schema
- [`db_get_table_path()`](https://cathalbyrnegit.github.io/datapond/reference/db_get_table_path.md)
  : Get the data path for a table
- [`db_hive_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_hive_read.md)
  : Read a CSO Dataset from hive-partitioned parquet (lazy)
- [`db_hive_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_hive_write.md)
  : Publish / Append / Ignore / Replace Partitions in the Hive Lake
- [`db_is_public()`](https://cathalbyrnegit.github.io/datapond/reference/db_is_public.md)
  : Check if a dataset is in the public catalog (Hive mode only)
- [`db_lake_connect()`](https://cathalbyrnegit.github.io/datapond/reference/db_lake_connect.md)
  : Connect to DuckDB + attach a DuckLake catalog
- [`db_lake_read()`](https://cathalbyrnegit.github.io/datapond/reference/db_lake_read.md)
  : Read a DuckLake table (lazy)
- [`db_lake_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_lake_write.md)
  : Write a DuckLake table (overwrite/append)
- [`db_list_datasets()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_datasets.md)
  : List datasets within a section
- [`db_list_public()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_public.md)
  : List all datasets in the public catalog (Hive mode only)
- [`db_list_schemas()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_schemas.md)
  : List schemas in the DuckLake catalog
- [`db_list_sections()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_sections.md)
  : List sections in the hive data lake
- [`db_list_tables()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_tables.md)
  : List tables in a DuckLake schema
- [`db_list_views()`](https://cathalbyrnegit.github.io/datapond/reference/db_list_views.md)
  : List views in a DuckLake schema
- [`db_preview_hive_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_hive_write.md)
  : Preview a hive write operation
- [`db_preview_lake_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_lake_write.md)
  : Preview a DuckLake write operation
- [`db_preview_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_preview_upsert.md)
  : Preview an upsert operation
- [`db_query()`](https://cathalbyrnegit.github.io/datapond/reference/db_query.md)
  : Run arbitrary SQL and return results
- [`db_rollback()`](https://cathalbyrnegit.github.io/datapond/reference/db_rollback.md)
  : Rollback a table to a previous snapshot
- [`db_search()`](https://cathalbyrnegit.github.io/datapond/reference/db_search.md)
  : Search for datasets or tables
- [`db_search_columns()`](https://cathalbyrnegit.github.io/datapond/reference/db_search_columns.md)
  : Search for columns
- [`db_set_private()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_private.md)
  : Remove a dataset from the public catalog (Hive mode only)
- [`db_set_public()`](https://cathalbyrnegit.github.io/datapond/reference/db_set_public.md)
  : Make a dataset discoverable in the public catalog (Hive mode only)
- [`db_snapshots()`](https://cathalbyrnegit.github.io/datapond/reference/db_snapshots.md)
  : List DuckLake snapshots
- [`db_status()`](https://cathalbyrnegit.github.io/datapond/reference/db_status.md)
  : Get connection status and configuration
- [`db_sync_catalog()`](https://cathalbyrnegit.github.io/datapond/reference/db_sync_catalog.md)
  : Sync the public catalog with source metadata (Hive mode only)
- [`db_table_cols()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_cols.md)
  : Get column names for a DuckLake table
- [`db_table_exists()`](https://cathalbyrnegit.github.io/datapond/reference/db_table_exists.md)
  : Check if a DuckLake table exists
- [`db_upsert()`](https://cathalbyrnegit.github.io/datapond/reference/db_upsert.md)
  : Upsert into a DuckLake table using MERGE INTO
- [`db_vacuum()`](https://cathalbyrnegit.github.io/datapond/reference/db_vacuum.md)
  : Vacuum old snapshots from DuckLake
- [`db_view_cols()`](https://cathalbyrnegit.github.io/datapond/reference/db_view_cols.md)
  : Get column names for a DuckLake view
- [`run_example()`](https://cathalbyrnegit.github.io/datapond/reference/run_example.md)
  : Run a package example script
