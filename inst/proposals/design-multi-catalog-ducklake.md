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

### Sync to Master

```r
# Sync current section's metadata to master
db_sync_to_master()
#> Synced 3 tables from 'trade' to master catalog

# Sync happens automatically on write (optional)
db_lake_write(data, table = "imports", sync_master = TRUE)

# Full refresh of master from all accessible sections
db_refresh_master()
```

---

## Implementation Details

### Helper Functions

```r
# Get master catalog connection
.db_master_connect <- function(master_path = NULL) {
  if (is.null(master_path)) {
    master_path <- getOption("datapond.master_catalog",
                             file.path(.db_get("data_path"), "..", "_master", "discovery.sqlite"))
  }
  DBI::dbConnect(RSQLite::SQLite(), master_path)
}

# Sync a table's metadata to master
.db_sync_table_to_master <- function(section, schema, table, master_con = NULL) {
  own_con <- is.null(master_con)
  if (own_con) {
    master_con <- .db_master_connect()
    on.exit(DBI::dbDisconnect(master_con))
  }

  # Get metadata from section catalog
  con <- .db_get_con()
  catalog <- .db_get("catalog")

  # Get table info
  cols <- DBI::dbGetQuery(con, glue::glue("
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND table_name = '{table}'
  "))

  # Get docs if available
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
    section,
    schema,
    table,
    docs$description,
    docs$owner,
    paste(docs$tags, collapse = ","),
    jsonlite::toJSON(cols),
    row_count,
    Sys.time()
  ))
}
```

### Modified db_lake_write

```r
db_lake_write <- function(..., sync_master = getOption("datapond.auto_sync_master", FALSE)) {
  # ... existing write logic ...

  # Optionally sync to master
  if (sync_master) {
    section <- .db_get("section")
    if (!is.null(section)) {
      tryCatch({
        .db_sync_table_to_master(section, schema, table)
      }, error = function(e) {
        warning("Failed to sync to master: ", e$message)
      })
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

**Phase 2: Discovery**
- [ ] `db_lake_discover()` - list all tables from master
- [ ] `db_lake_search_all()` - search across sections
- [ ] `db_lake_table_info()` - detailed info from master

**Phase 3: Sync**
- [ ] `db_sync_to_master()` - manual sync
- [ ] `sync_master` parameter on `db_lake_write()`
- [ ] `db_refresh_master()` - full refresh

**Phase 4: Browser Integration**
- [ ] Section selector in browser
- [ ] "All Sections" discovery view
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

# User: Discover what's available
db_lake_discover()
#> Shows all tables from master (even ones you can't access)

# User: Connect to section you have access to
db_lake_connect_section("trade")

# Work with data
imports <- db_lake_read(table = "imports")
imports |>
  filter(year == 2024) |>
  collect()

# Write data (optionally syncs to master)
db_lake_write(new_data, table = "imports", sync_master = TRUE)

# Switch to another section
db_switch_section("shared")
countries <- db_lake_read(table = "countries")
```

---

## Relationship to Public Catalog (Hive Mode)

The master discovery catalog for DuckLake serves a similar purpose to the `_catalog/` folder for Hive mode:

| Aspect | Hive Public Catalog | DuckLake Master Catalog |
|--------|---------------------|------------------------|
| Storage | JSON files in `_catalog/` | SQLite database |
| Scope | Per-dataset opt-in | Per-section registration |
| Sync | Via `db_describe(public=TRUE)` | Via `db_sync_to_master()` |
| Discovery | `db_list_public()` | `db_lake_discover()` |
| Search | `db_search()` with public | `db_lake_search_all()` |

Both solve the same fundamental problem: **enabling organisation-wide data discovery while maintaining data-level access control**.
