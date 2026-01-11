# Preview a hive write operation

Shows what would happen if you ran
[`db_hive_write()`](https://cathalbyrnegit.github.io/datapond/reference/db_hive_write.md)
without actually writing any data. Useful for validating writes before
execution.

## Usage

``` r
db_preview_hive_write(
  data,
  section,
  dataset,
  partition_by = NULL,
  mode = c("overwrite", "append", "ignore", "replace_partitions")
)
```

## Arguments

- data:

  A data.frame / tibble

- section:

  Your section name

- dataset:

  The name of the dataset

- partition_by:

  Character vector of column names to partition by (e.g.
  c("year","month"))

- mode:

  One of:

  - "overwrite": replace target files

  - "append": add new files (requires unique filenames)

  - "ignore": write only if target path does not exist (best-effort;
    still race-prone)

  - "replace_partitions": delete only affected partition folders, then
    append fresh files (requires partition_by)

## Value

A list with preview information (invisibly), also prints a summary

## Examples

``` r
if (FALSE) { # \dontrun{
db_connect()

# Preview before writing
db_preview_hive_write(my_data, "Trade", "Imports", partition_by = "year")

# If preview looks good, actually write
db_hive_write(my_data, "Trade", "Imports", partition_by = "year")
} # }
```
