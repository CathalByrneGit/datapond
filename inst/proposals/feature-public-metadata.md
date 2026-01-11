# Feature: Public Metadata Catalog for Hive Mode

## Status: Implemented

This feature has been implemented in `R/docs.R`, `R/browser_ui.R`, and `R/browser_server.R`.

---

## Overview

This feature enables a **universal metadata viewer** in Hive mode while maintaining data security isolation. Datasets can be marked as "public" (discoverable) without granting access to the underlying data.

---

## Problem Statement

In Hive mode, data security works perfectly via folder permissions. However, this creates a discovery problem:

- Users can only see metadata for datasets they have folder access to
- There's no way to discover what datasets exist across the organisation
- A central data catalog/browser cannot show all available datasets
- Users must "know" what to ask for rather than being able to browse

**Solution**: Copy metadata to a shared `_catalog/` folder that everyone can read, enabling organisation-wide data discovery while maintaining data-level security.

---

## Implementation

### Catalog Folder Structure

```
//CSO-NAS/DataLake/
├── _catalog/                           ← Shared catalog folder (everyone has read access)
│   ├── Trade/
│   │   ├── Imports.json                ← Copy of metadata (public)
│   │   └── Exports.json                ← Copy of metadata (public)
│   └── Labour/
│       └── Employment.json             ← Copy of metadata (public)
│
├── Trade/                              ← Actual data (restricted access)
│   ├── Imports/
│   │   ├── year=2024/...
│   │   └── _metadata.json              ← Source metadata
│   └── Exports/
│       └── ...
└── Labour/                             ← Actual data (restricted access)
    └── ...
```

---

## API

### Primary: Using db_describe()

The recommended way to manage public status is through the `public` parameter on `db_describe()`:

```r
# Make a dataset public when documenting it
db_describe(
  section = "Trade",
  dataset = "Imports",
  description = "Monthly import values by country and commodity code",
  owner = "Trade Section",
  tags = c("trade", "monthly", "official"),
  public = TRUE
)

# Remove from public catalog
db_describe(
  section = "Trade",
  dataset = "Imports",
  public = FALSE
)

# Update metadata (auto-syncs if already public)
db_describe(
  section = "Trade",
  dataset = "Imports",
  description = "Updated description"
  # public = NULL (default) - keeps current status and syncs if public
)
```

### Column Documentation

Column documentation also supports the `public` parameter, but requires the dataset to already be public:

```r
# Document a column and sync to public catalog
db_describe_column(
  section = "Trade",
  dataset = "Imports",
  column = "value",
  description = "Import value in thousands",
  units = "EUR (thousands)",
  public = TRUE  # Only works if dataset is already public
)

# Auto-sync: if dataset is public, column docs are synced automatically
db_describe_column(
  section = "Trade",
  dataset = "Imports",
  column = "country_code",
  description = "ISO 3166-1 alpha-2 country code"
  # public = NULL (default) - auto-syncs if dataset is public
)
```

### Convenience Functions

Explicit functions for managing public status:

```r
# Make an existing dataset public
db_set_public(section = "Trade", dataset = "Imports")

# Make a dataset private (remove from catalog)
db_set_private(section = "Trade", dataset = "Imports")

# Check if a dataset is public
db_is_public(section = "Trade", dataset = "Imports")
#> [1] TRUE

# List all public datasets
db_list_public()
#>   section   dataset                    description         owner          tags
#> 1   Trade   Imports  Monthly import values by...   Trade Section  trade, monthly

# Filter by section
db_list_public(section = "Trade")
```

### Catalog Maintenance

```r
# Sync all public catalog entries with their sources
db_sync_catalog()
#> Sync complete: 5 synced, 0 removed, 0 errors

# Remove orphan entries (where source dataset was deleted)
db_sync_catalog(remove_orphans = TRUE)
#> Removed orphan: Archive/OldData
#> Sync complete: 4 synced, 1 removed, 0 errors
```

---

## Browser Integration

The `db_browser()` Shiny app includes a **Public Catalog** tab (hive mode only) with:

1. **Public Datasets Table**: View all datasets in the public catalog
2. **Refresh Button**: Reload the catalog list
3. **Sync All Button**: Sync all public entries with source metadata
4. **Public Status Display**: Shows whether selected dataset is public/private
5. **Make Public/Private Buttons**: Quick actions to toggle public status

Additionally:
- The **Metadata** tab shows a "Public" badge for public datasets
- The metadata card displays public status

---

## Auto-Sync Behaviour

The system automatically syncs public metadata in these situations:

| Action | Behaviour |
|--------|-----------|
| `db_describe(public = TRUE)` | Publishes to catalog |
| `db_describe(public = FALSE)` | Removes from catalog |
| `db_describe()` (no public param) | Syncs if already public |
| `db_describe_column(public = TRUE)` | Syncs if dataset is public (error if not) |
| `db_describe_column()` (no public param) | Syncs if dataset is public |

This ensures the public catalog stays current without manual intervention.

---

## Security Model

### What This Enables
- Users can discover ALL public datasets organisation-wide
- Users can see descriptions, owners, tags, column documentation
- Users can contact dataset owners to request access

### What This Does NOT Enable
- Users cannot read data from datasets they don't have folder access to
- The underlying security model remains unchanged
- Only **metadata** is shared, not data

### Permissions Model
```
//CSO-NAS/DataLake/
├── _catalog/          ← Everyone: Read-only
├── Trade/             ← Trade team: Read-write
├── Labour/            ← Labour team: Read-write
└── Shared/            ← Everyone: Read (or read-write)
```

---

## Internal Implementation

### Helper Functions (in R/docs.R)

```r
.db_catalog_path()              # Get path to _catalog folder
.db_public_metadata_path()      # Get path to public JSON file
.db_publish_metadata()          # Copy metadata to catalog
.db_unpublish_metadata()        # Remove from catalog
.db_sync_public_catalog()       # Sync if already public
```

### Public JSON Format

When metadata is published, additional fields are added:

```json
{
  "description": "Monthly import values by country and commodity code",
  "owner": "Trade Section",
  "tags": ["trade", "monthly", "official"],
  "columns": {
    "value": {
      "description": "Import value in thousands",
      "units": "EUR (thousands)"
    }
  },
  "created_at": "2025-01-10 10:00:00",
  "updated_at": "2025-01-11 14:30:00",
  "section": "Trade",
  "dataset": "Imports",
  "public": true,
  "catalog_published_at": "2025-01-11 14:30:00"
}
```

---

## Migration Guide

### Making Existing Datasets Public

```r
db_connect("//CSO-NAS/DataLake")

# Option 1: One at a time
db_set_public("Trade", "Imports")
db_set_public("Trade", "Exports")

# Option 2: Bulk publish all documented datasets
for (section in db_list_sections()) {
  for (dataset in db_list_datasets(section)) {
    docs <- tryCatch(db_get_docs(section, dataset), error = function(e) list())
    if (!is.null(docs$description)) {
      db_set_public(section, dataset)
    }
  }
}
```

---

## Functions Added

| Function | Description |
|----------|-------------|
| `db_set_public()` | Publish dataset metadata to catalog |
| `db_set_private()` | Remove dataset metadata from catalog |
| `db_is_public()` | Check if dataset is in catalog |
| `db_list_public()` | List all public datasets |
| `db_sync_catalog()` | Sync/cleanup the public catalog |

## Functions Modified

| Function | Change |
|----------|--------|
| `db_describe()` | Added `public` parameter |
| `db_describe_column()` | Added `public` parameter with table-must-be-public check |
| `db_browser_ui()` | Added "Public Catalog" tab |
| `db_browser_server()` | Added public catalog handlers |
| `.render_metadata_card()` | Added public badge display |
