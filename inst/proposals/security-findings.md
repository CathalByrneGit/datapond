# Security Findings: File Permission Integration

## Executive Summary

This document analyses how datapond's two storage modes integrate with existing file permission systems for data security.

**Key Finding**: Hive mode provides complete security isolation via folder permissions. DuckLake mode has a fundamental architectural limitation where file permissions cannot provide equivalent schema-level isolation.

---

## Background

A key design goal of datapond is integration with existing IT security infrastructure, specifically file system permissions (ACLs). The expectation is that teams can control data access by setting folder permissions on network shares.

---

## Hive Mode: Complete File Permission Security

### How It Works

In Hive mode, the data structure maps directly to the folder structure:

```
//CSO-NAS/DataLake/
├── Trade/                          ← Section folder (ACL controlled)
│   ├── Imports/                    ← Dataset folder
│   │   ├── year=2024/month=01/
│   │   │   └── data.parquet
│   │   └── _metadata.json          ← Documentation sidecar
│   └── Exports/
│       └── ...
├── Labour/                         ← Section folder (ACL controlled)
│   └── Employment/
│       └── ...
└── Shared/                         ← Everyone has access
    └── Reference/
        └── ...
```

### Security Model

| Operation | Implementation | Security Source |
|-----------|----------------|-----------------|
| `db_list_sections()` | `list.dirs(base_path)` | File system ACLs |
| `db_list_datasets(section)` | `list.dirs(section_path)` | File system ACLs |
| `db_hive_read(section, dataset)` | DuckDB reads parquet via file path | File system ACLs |
| `db_get_docs(section, dataset)` | Reads `_metadata.json` file | File system ACLs |
| `db_search()` | Scans `_metadata.json` files | File system ACLs |

### Security Outcome

If a user lacks permission to `//CSO-NAS/DataLake/Trade/`:
- They **cannot** list datasets in Trade (folder listing fails)
- They **cannot** read Trade data (DuckDB cannot access parquet files)
- They **cannot** see Trade documentation (cannot read `_metadata.json`)
- They **do not know** Trade exists (unless they see the folder name in a higher-level listing)

**Verdict**: Complete security isolation via existing file permissions.

---

## DuckLake Mode: Split Security Model

### How It Works

DuckLake separates metadata (catalog) from data (parquet files):

```
//CSO-NAS/DataLake/
├── catalog.sqlite              ← Single shared metadata file
└── data/                       ← Single data folder
    ├── 550e8400-e29b-41d4-a716-446655440000.parquet
    ├── 6ba7b810-9dad-11d1-80b4-00c04fd430c8.parquet
    └── ... (flat structure, UUID-based names)
```

**Critical**: DuckLake uses a **flat file structure with UUID-based naming** rather than schema-based folders. The schema/table structure exists **only in the metadata catalog**.

Source: [DuckLake Documentation](https://duckdb.org/docs/stable/core_extensions/ducklake)

### Security Model

| Operation | Implementation | Security Source |
|-----------|----------------|-----------------|
| `db_list_schemas()` | SQL query to catalog | Catalog access only |
| `db_list_tables(schema)` | SQL query to catalog | Catalog access only |
| `db_lake_read(schema, table)` | DuckDB via catalog → parquet | **Both** catalog + data folder |
| `db_get_docs(schema, table)` | SQL query to `_metadata.table_docs` | Catalog access only |
| `db_search()` | SQL query to catalog | Catalog access only |
| `db_snapshots()` | SQL query to catalog | Catalog access only |

### The Problem

Everyone needs read access to `catalog.sqlite` to query **any** table. This creates metadata leakage:

| What Users Can See | With Catalog Access | Without Data Folder Access |
|--------------------|---------------------|---------------------------|
| Table names in all schemas | Yes | Yes |
| Column names and types | Yes | Yes |
| Documentation (descriptions, owners, tags) | Yes | Yes |
| Audit trail (who wrote what, when) | Yes | Yes |
| Actual data rows | No | **Blocked** |

### Security Outcome

If a user has catalog access but lacks permission to specific parquet files:
- They **can** see what tables exist in all schemas
- They **can** see table schemas (columns and types)
- They **can** see all documentation
- They **can** see commit history and audit trail
- They **cannot** read actual data rows (access error)

**Verdict**: Partial security. Data is protected but metadata is exposed.

### Why Schema Folders Don't Exist

Unlike the diagrams in the vignettes (which were illustrative), DuckLake deliberately uses flat file storage:

> "DuckLake uses a flat file structure with UUID-based naming rather than hierarchical folders. This simplifies operations like table renaming and avoids binding files to structures that may need to change."

This is a deliberate architectural choice by DuckLake, not something datapond can override.

---

## Comparison Matrix

| Aspect | Hive Mode | DuckLake Mode |
|--------|-----------|---------------|
| **Discovery security** | Per-section (folder ACLs) | None (shared catalog) |
| **Schema visibility** | Per-section | All schemas visible |
| **Documentation security** | Per-dataset (JSON in folder) | None (shared catalog) |
| **Audit trail security** | N/A (no audit trail) | None (shared catalog) |
| **Data row security** | Per-folder | Per-file (flat structure) |
| **File permission model** | Section → folder mapping | Single data folder |

---

## Root Cause Analysis

The fundamental issue is architectural:

1. **Hive mode**: Discovery is file-based → file permissions control visibility
2. **DuckLake mode**: Discovery is catalog-based → catalog must be shared

DuckLake was designed for cloud data lakes where:
- Storage (S3/GCS/Azure) handles data access via IAM policies
- The catalog is typically a managed database with its own auth layer
- Row-level or column-level security is handled at the query engine level

When used with network file shares:
- File ACLs can protect data files
- But there's no equivalent mechanism to protect catalog metadata
- The catalog file must be readable by all users who need any access

---

## Implications

### For Organisations Requiring Schema-Level Isolation

DuckLake in its current form **cannot provide** the same security isolation as Hive mode when relying solely on file permissions.

### Acceptable Use Cases for DuckLake

DuckLake remains appropriate when:
- Metadata visibility is acceptable (knowing tables exist isn't sensitive)
- All users should have access to all schemas
- Security is at the data row level, not discovery level
- The time travel / ACID / audit features outweigh isolation concerns

---

## Remediation Options

See [feature-public-metadata.md](feature-public-metadata.md) for proposed enhancements.

### Short Term

1. **Multiple DuckLake catalogs** - Create separate catalogs per security boundary
2. **Hybrid approach** - Use Hive for isolated data, DuckLake for shared data

### Long Term

1. **PostgreSQL catalog with RLS** - Row-level security on metadata tables
2. **Catalog middleware** - Application-level access control

---

## References

- [DuckLake Documentation](https://duckdb.org/docs/stable/core_extensions/ducklake)
- [DuckLake GitHub](https://github.com/duckdb/ducklake)
- [DuckLake Announcement](https://ducklake.select/2025/05/27/ducklake-01/)
