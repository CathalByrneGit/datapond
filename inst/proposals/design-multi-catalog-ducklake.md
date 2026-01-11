# Design Proposal: Multi-Catalog DuckLake with Master Discovery

## Status: Proposal

---

## Problem Statement

DuckLake uses a **flat file structure with UUID-based naming**. This means:
- All parquet files go into a single `data/` folder regardless of schema
- File permissions cannot provide schema-level isolation
- Everyone with catalog access can see all table metadata

For organisations requiring section-level data isolation, the single-catalog model doesn't integrate with existing file permission systems.

---

## Proposed Architecture

### Multi-Catalog with Master Discovery

```
//CSO-NAS/DataLake/
├── _master/
│   └── discovery.sqlite          ← Master discovery catalog (everyone: read)
│
├── trade/                        ← Trade team only
│   ├── catalog.sqlite            ← DuckLake catalog
│   └── data/
│       ├── abc123.parquet
│       └── def456.parquet
│
├── labour/                       ← Labour team only
│   ├── catalog.sqlite            ← DuckLake catalog
│   └── data/
│       └── ...
│
└── shared/                       ← Everyone
    ├── catalog.sqlite            ← DuckLake catalog
    └── data/
        └── ...
```

### Security Model

| Folder | Trade Team | Labour Team | Everyone |
|--------|------------|-------------|----------|
| `_master/` | Read | Read | Read |
| `trade/` | Read/Write | No Access | No Access |
| `labour/` | No Access | Read/Write | No Access |
| `shared/` | Read/Write | Read/Write | Read |

### Key Benefits

1. **Section isolation**: Each section has its own DuckLake catalog and data folder
2. **File permission integration**: Standard folder ACLs control access
3. **Organisation-wide discovery**: Master catalog allows finding what exists
4. **DuckLake features preserved**: Time travel, ACID, etc. within each section

---

## Master Discovery Catalog

The master catalog is NOT a DuckLake catalog - it's a simple SQLite database storing metadata only.

### Schema

```sql
-- Registered sections
CREATE TABLE sections (
    section_name TEXT PRIMARY KEY,
    catalog_path TEXT NOT NULL,
    data_path TEXT NOT NULL,
    description TEXT,
    owner TEXT,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table registry (synced from section catalogs)
CREATE TABLE tables (
    section_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    description TEXT,
    owner TEXT,
    tags TEXT,
    columns_json TEXT,           -- JSON array of {name, type, description}
    row_count INTEGER,
    last_updated TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (section_name, schema_name, table_name),
    FOREIGN KEY (section_name) REFERENCES sections(section_name)
);

-- Optional: snapshot history summary
CREATE TABLE snapshots_summary (
    section_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    latest_version INTEGER,
    latest_timestamp TIMESTAMP,
    total_snapshots INTEGER,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### What Master Contains vs Doesn't Contain

| Contains | Doesn't Contain |
|----------|-----------------|
| Table names and schemas | Actual data rows |
| Column names and types | Parquet file contents |
| Documentation (descriptions, tags) | Detailed snapshot history |
| Row counts (approximate) | Query capabilities |
| Last update timestamps | Time travel data |

---

## API Design

### Section Registration

```r
# Register a section with the master catalog
db_register_section(
  section = "trade",
  catalog_path = "//CSO-NAS/DataLake/trade/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/trade/data",
  description = "Trade statistics section",
  owner = "Trade Team"
)

# List registered sections
db_list_registered_sections()
#>   section_name                           catalog_path            owner
#> 1        trade  //CSO-NAS/DataLake/trade/catalog.sqlite       Trade Team
#> 2       labour //CSO-NAS/DataLake/labour/catalog.sqlite      Labour Team
#> 3       shared //CSO-NAS/DataLake/shared/catalog.sqlite         Everyone
```

### Section Connection

```r
# Connect to a specific section
db_lake_connect_section(
  section = "trade",
  master_path = "//CSO-NAS/DataLake/_master/discovery.sqlite"
)
# Automatically reads catalog_path and data_path from master

# Or connect directly without master
db_lake_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/trade/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/trade/data"
)

# Check current section
db_current_section()
#> [1] "trade"

# Switch sections
db_switch_section("labour")
```

### Organisation-Wide Discovery

```r
# Discover all tables across all sections (reads master only)
db_lake_discover()
#>   section_name schema_name table_name               description        owner
#> 1        trade        main    imports  Monthly import statistics  Trade Team
#> 2        trade        main    exports  Monthly export statistics  Trade Team
#> 3       labour        main employment         Employment figures Labour Team
#> 4       shared        main  countries        Country reference       Everyone

# Search across all sections
db_lake_search_all("monthly")
#>   section_name schema_name table_name               description
#> 1        trade        main    imports  Monthly import statistics
#> 2        trade        main    exports  Monthly export statistics

# Get detailed info from master
db_lake_table_info(section = "trade", schema = "main", table = "imports")
#> $description
#> [1] "Monthly import statistics"
#> $columns
#>     name    type         description
#> 1   year INTEGER        Calendar year
#> 2  month INTEGER       Month (1-12)
#> 3  value  DOUBLE  Import value (EUR)
```

### Publishing to Master (via db_describe)

The API uses the same `public` parameter pattern as Hive mode for consistency:

```r
# Make a table public (syncs to master discovery catalog)
db_describe(
  schema = "main",
  table = "imports",
  description = "Monthly import statistics",
  owner = "Trade Team",
  tags = c("trade", "monthly"),
  public = TRUE
)

# Remove from master discovery catalog
db_describe(
  schema = "main",
  table = "imports",
  public = FALSE
)

# Update description (auto-syncs if already public)
db_describe(
  schema = "main",
  table = "imports",
  description = "Updated description"
  # public = NULL (default) - keeps current status and syncs if public
)

# Convenience functions (same as Hive mode)
db_set_public(schema = "main", table = "imports")
db_set_private(schema = "main", table = "imports")
db_is_public(schema = "main", table = "imports")
db_list_public()  # Lists all public tables from master
```

### Catalog Maintenance

```r
# Sync all public entries with their source metadata
db_sync_catalog()
#> Sync complete: 5 synced, 0 removed, 0 errors

# Remove orphan entries (where source table was deleted)
db_sync_catalog(remove_orphans = TRUE)
```

---

## Implementation Details

### Unified Public Parameter Approach

The key insight is that DuckLake mode should use the **same API pattern** as Hive mode. The `public` parameter on `db_describe()` controls whether metadata is published to the discovery catalog:

| Mode | Source Metadata | Public Catalog | Sync Trigger |
|------|-----------------|----------------|--------------|
| Hive | `_metadata.json` in dataset folder | JSON in `_catalog/` folder | `db_describe(public=TRUE)` |
| DuckLake | DuckDB catalog tables | SQLite master database | `db_describe(public=TRUE)` |

This means:
- **No separate sync functions needed** - `db_describe()` handles it
- **Consistent API** across both modes
- **Auto-sync behaviour** works the same way

### Helper Functions

```r
# Get master catalog connection (DuckLake mode)
.db_master_connect <- function(master_path = NULL) {
  if (is.null(master_path)) {
    master_path <- getOption("datapond.master_catalog",
                             file.path(.db_get("data_path"), "..", "_master", "discovery.sqlite"))
  }
  DBI::dbConnect(RSQLite::SQLite(), master_path)
}

# Publish table metadata to master (called by db_describe when public=TRUE)
.db_publish_to_master <- function(schema, table) {
  section <- .db_get("section")
  master_con <- .db_master_connect()
  on.exit(DBI::dbDisconnect(master_con))

  # Get column info from DuckLake catalog
  con <- .db_get_con()
  catalog <- .db_get("catalog")
  cols <- DBI::dbGetQuery(con, glue::glue("
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND table_name = '{table}'
  "))

  # Get docs
  docs <- tryCatch(db_get_docs(schema = schema, table = table), error = function(e) list())

  # Get row count
  row_count <- tryCatch({
    DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) as n FROM {catalog}.{schema}.{table}"))$n
  }, error = function(e) NA_integer_)

  # Upsert to master

  DBI::dbExecute(master_con, "
    INSERT OR REPLACE INTO tables
    (section_name, schema_name, table_name, description, owner, tags, columns_json, row_count, last_updated, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
  ", params = list(
    section, schema, table,
    docs$description, docs$owner, paste(docs$tags, collapse = ","),
    jsonlite::toJSON(cols), row_count, Sys.time()
  ))
}

# Remove table from master (called by db_describe when public=FALSE)
.db_unpublish_from_master <- function(schema, table) {
  section <- .db_get("section")
  master_con <- .db_master_connect()
  on.exit(DBI::dbDisconnect(master_con))

  DBI::dbExecute(master_con, "
    DELETE FROM tables
    WHERE section_name = ? AND schema_name = ? AND table_name = ?
  ", params = list(section, schema, table))
}
```

### Modified db_describe (DuckLake mode)

```r
db_describe <- function(schema = "main", table = NULL, ..., public = NULL) {
  # ... existing documentation logic ...

  # Handle public parameter (same logic as Hive mode)
  if (isTRUE(public)) {
    .db_publish_to_master(schema, table)
  } else if (isFALSE(public)) {
    .db_unpublish_from_master(schema, table)
  } else if (is.null(public)) {
    # Auto-sync if already public
    if (.db_is_public_in_master(schema, table)) {
      .db_publish_to_master(schema, table)
    }
  }
}
```

---

## Alternative Approaches Considered

### Alternative 1: View-Based Federation

Use DuckDB's ability to attach multiple databases and create views.

```r
# Attach all accessible section catalogs
db_lake_connect_federated(sections = c("trade", "shared"))
# Creates views like: all_tables.trade_imports, all_tables.shared_countries
```

**Pros**: True cross-section queries possible
**Cons**: Complex setup, permission errors if section inaccessible, DuckLake doesn't support this well

### Alternative 2: Single Catalog with Metadata-Based Access Control

Keep single catalog but add application-level access control.

```r
# Check permissions before returning data
db_lake_read <- function(schema, table) {
  if (!.db_user_can_access(schema, table)) {
    stop("Access denied to ", schema, ".", table)
  }
  # ... read data ...
}
```

**Pros**: Simpler architecture, cross-section queries work
**Cons**: Doesn't prevent direct file access, security through obscurity

### Alternative 3: Separate Catalogs, No Master

Just use separate catalogs without centralised discovery.

```r
db_lake_connect(
  catalog_type = "sqlite",
  metadata_path = "//CSO-NAS/DataLake/trade/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/trade/data"
)
```

**Pros**: Simple, file permissions work
**Cons**: No organisation-wide discovery, users need to know what exists

---

## Comparison Matrix

| Aspect | Single Catalog | Multi-Catalog (No Master) | Multi-Catalog + Master |
|--------|----------------|---------------------------|------------------------|
| Section isolation | No | Yes | Yes |
| File permission integration | No | Yes | Yes |
| Organisation-wide discovery | Yes (metadata leakage) | No | Yes (controlled) |
| Cross-section queries | Yes | No | No |
| Complexity | Low | Low | Medium |
| DuckLake features | Full | Full per-section | Full per-section |

---

## Recommended Approach

### For Most Use Cases: Multi-Catalog + Master

1. **Each section gets its own DuckLake catalog** in a folder with appropriate permissions
2. **Master discovery catalog** provides organisation-wide visibility
3. **Sync on demand** keeps master up to date
4. **Connection helpers** make switching sections easy

### Implementation Phases

**Phase 1: Foundation**
- [ ] `db_register_section()` / `db_unregister_section()`
- [ ] `db_list_registered_sections()`
- [ ] Master catalog schema creation
- [ ] `db_lake_connect_section()` using master

**Phase 2: Public Catalog (Unified API)**
- [ ] Extend `db_describe()` with `public` parameter for DuckLake mode
- [ ] Extend `db_set_public()` / `db_set_private()` for DuckLake mode
- [ ] Extend `db_is_public()` for DuckLake mode
- [ ] Extend `db_list_public()` to read from master catalog
- [ ] Extend `db_sync_catalog()` for DuckLake mode

**Phase 3: Discovery**
- [ ] `db_lake_discover()` - alias for `db_list_public()` in DuckLake mode
- [ ] Search functionality across sections

**Phase 4: Browser Integration**
- [ ] Section selector in browser
- [ ] "All Sections" discovery view (reuse Public Catalog tab)
- [ ] Switch section from browser

---

## Open Questions

1. **Automatic sync timing**: On every write? Periodic? Manual only?
2. **Stale data handling**: How long before master data considered stale?
3. **Section creation**: Should package create section structure or expect it to exist?
4. **Cross-section references**: Should master track foreign key relationships?
5. **Permissions in master**: Should master store who can access what?

---

## Example Workflow

```r
library(datapond)

# Admin: Set up sections (one-time)
db_setup_master("//CSO-NAS/DataLake/_master/discovery.sqlite")

db_register_section(
  section = "trade",
  catalog_path = "//CSO-NAS/DataLake/trade/catalog.sqlite",
  data_path = "//CSO-NAS/DataLake/trade/data",
  owner = "Trade Team"
)

# User: Discover what's available (same function as Hive mode!)
db_list_public()
#> Shows all public tables from master (even ones you can't access)

# User: Connect to section you have access to
db_lake_connect_section("trade")

# Work with data
imports <- db_lake_read(table = "imports")
imports |>
  filter(year == 2024) |>
  collect()

# Write data
db_lake_write(new_data, table = "imports")

# Document and make public (same API as Hive mode!)
db_describe(
  table = "imports",
  description = "Monthly import statistics",
  owner = "Trade Team",
  public = TRUE
)

# Switch to another section
db_switch_section("shared")
countries <- db_lake_read(table = "countries")
```

---

## Unified API with Hive Mode

The key design principle is that DuckLake mode uses the **same API** as Hive mode for public metadata management. This provides a consistent developer experience regardless of storage backend:

| Function | Hive Mode | DuckLake Mode |
|----------|-----------|---------------|
| `db_describe(public=TRUE)` | Copies JSON to `_catalog/` | Writes to master SQLite |
| `db_describe(public=FALSE)` | Removes from `_catalog/` | Removes from master SQLite |
| `db_set_public()` | Convenience wrapper | Convenience wrapper |
| `db_set_private()` | Convenience wrapper | Convenience wrapper |
| `db_is_public()` | Checks `_catalog/` exists | Checks master table |
| `db_list_public()` | Lists `_catalog/` contents | Queries master SQLite |
| `db_sync_catalog()` | Syncs all public JSON files | Syncs all master entries |

### Storage Comparison

| Aspect | Hive Mode | DuckLake Mode |
|--------|-----------|---------------|
| Source metadata | `_metadata.json` per dataset | DuckDB catalog tables |
| Public catalog storage | JSON files in `_catalog/` folder | SQLite database in `_master/` |
| Folder structure | Mirrors section/dataset hierarchy | Flat table with section column |
| Access control | Folder ACLs on `_catalog/` | File ACL on `discovery.sqlite` |

Both solve the same fundamental problem: **enabling organisation-wide data discovery while maintaining data-level access control**.
