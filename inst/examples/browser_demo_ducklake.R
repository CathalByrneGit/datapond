# browser_demo_ducklake.R
# ========================
# Interactive demo of db_browser() with sample DuckLake data
# Demonstrates multi-catalog architecture with master discovery catalog
#
# Run with:
#   source(system.file("examples", "browser_demo_ducklake.R", package = "datapond"))
#
# Or use the helper:
#   datapond::run_example("browser_demo_ducklake")

library(datapond)

# Create a temp directory for our test data lake
lake_path <- file.path(tempdir(), "test_ducklake")
if (dir.exists(lake_path)) unlink(lake_path, recursive = TRUE)
dir.create(lake_path)

cat("Setting up DuckLake at:", lake_path, "\n\n")

# =============================================================================
# Set up Master Discovery Catalog (organisation-wide)
# =============================================================================
# The master catalog is a lightweight SQLite database that indexes public
# tables across all section catalogs for organisation-wide discovery.

master_path <- file.path(lake_path, "_master", "discovery.sqlite")
db_setup_master(master_path)
cat("✓ Master discovery catalog created\n")

# Set as default for this session
options(datapond.master_catalog = master_path)

# =============================================================================
# Create section directories and connect
# =============================================================================
# Each section has its own DuckLake catalog. First create the catalog,
# then register with master.

# Create directories for the section
section_catalog <- file.path(lake_path, "demo", "catalog.ducklake")
section_data <- file.path(lake_path, "demo", "data")
dir.create(dirname(section_catalog), recursive = TRUE, showWarnings = FALSE)
dir.create(section_data, recursive = TRUE, showWarnings = FALSE)

# Connect to create the catalog (this creates the .ducklake file)
db_lake_connect(
  catalog = "demo",
  metadata_path = section_catalog,
  data_path = section_data
)
cat("✓ Section catalog created\n")

# Store section name for public catalog operations
assign("section", "demo", envir = datapond:::.db_env)

# Register section in master catalog for discovery
db_register_section(
  section = "demo",
  catalog_path = section_catalog,
  data_path = section_data,
  description = "Demo section with sample data",
  owner = "Demo Team",
  master_path = master_path
)
cat("✓ Section 'demo' registered in master catalog\n")

# =============================================================================
# Create schemas and tables
# =============================================================================

db_create_schema("trade")
db_create_schema("labour")
db_create_schema("health")
db_create_schema("reference")

cat("✓ Created schemas\n")

# trade.imports
imports <- data.frame(
  year = rep(2023:2024, each = 12),
  month = rep(1:12, 2),
  country = sample(c("DE", "FR", "UK", "US", "CN", "JP"), 24, replace = TRUE),
  commodity = sample(c("Machinery", "Chemicals", "Food", "Textiles", "Electronics"), 24, replace = TRUE),
  value = round(runif(24, 100, 10000), 2),
  quantity = sample(100:5000, 24)
)

db_lake_write(imports, schema = "trade", table = "imports",
              commit_author = "demo", commit_message = "Initial load")
cat("✓ Created trade.imports\n")

# trade.exports
exports <- data.frame(
  year = rep(2023:2024, each = 12),
  month = rep(1:12, 2),
  country = sample(c("DE", "FR", "UK", "US", "CN", "JP"), 24, replace = TRUE),
  commodity = sample(c("Dairy", "Beef", "Pharma", "Software", "Machinery"), 24, replace = TRUE),
  value = round(runif(24, 50, 8000), 2),
  quantity = sample(50:3000, 24)
)

db_lake_write(exports, schema = "trade", table = "exports",
              commit_author = "demo", commit_message = "Initial load")
cat("✓ Created trade.exports\n")

# labour.employment
employment <- data.frame(
  year = rep(2022:2024, each = 4),
  quarter = rep(1:4, 3),
  sector = rep(c("Agriculture", "Manufacturing", "Services", "Public"), 3),
  employed = sample(50000:500000, 12),
  unemployed = sample(5000:50000, 12),
  participation_rate = round(runif(12, 0.55, 0.75), 3)
)

db_lake_write(employment, schema = "labour", table = "employment",
              commit_author = "demo", commit_message = "Initial load")
cat("✓ Created labour.employment\n")

# health.hospitals
hospitals <- data.frame(
  hospital_id = paste0("H", sprintf("%03d", 1:20)),
  name = paste("Hospital", LETTERS[1:20]),
  county = sample(c("Dublin", "Cork", "Galway", "Limerick", "Waterford"), 20, replace = TRUE),
  beds = sample(50:500, 20),
  staff = sample(100:2000, 20),
  type = sample(c("General", "Specialist", "Teaching"), 20, replace = TRUE)
)

db_lake_write(hospitals, schema = "health", table = "hospitals",
              commit_author = "demo", commit_message = "Initial load")
cat("✓ Created health.hospitals\n")

# reference.countries
countries <- data.frame(
  code = c("DE", "FR", "UK", "US", "CN", "JP", "IE"),
  name = c("Germany", "France", "United Kingdom", "United States", "China", "Japan", "Ireland"),
  region = c("Europe", "Europe", "Europe", "Americas", "Asia", "Asia", "Europe")
)

db_lake_write(countries, schema = "reference", table = "countries",
              commit_author = "demo", commit_message = "Initial load")
cat("✓ Created reference.countries\n")

# =============================================================================
# Add documentation
# =============================================================================

cat("\nAdding documentation...\n")

db_describe(
  schema = "trade",
  table = "imports",
  description = "Monthly import values by country and commodity",
  owner = "Trade Section",
  tags = c("trade", "monthly", "imports")
)

db_describe_column(schema = "trade", table = "imports",
                   column = "value",
                   description = "Import value",
                   units = "EUR (thousands)")

db_describe(
  schema = "trade",
  table = "exports",
  description = "Monthly export values by country and commodity",
  owner = "Trade Section",
  tags = c("trade", "monthly", "exports")
)

db_describe(
  schema = "labour",
  table = "employment",
  description = "Quarterly employment statistics by sector",
  owner = "Labour Market Section",
  tags = c("labour", "quarterly", "LFS")
)

db_describe(
  schema = "health",
  table = "hospitals",
  description = "Hospital reference data",
  owner = "Health Section",
  tags = c("health", "reference")
)

db_describe(
  schema = "reference",
  table = "countries",
  description = "Country code lookup table",
  owner = "Data Governance",
  tags = c("reference", "lookup")
)

cat("✓ Documentation added\n")

# =============================================================================
# Publish tables to Master Catalog
# =============================================================================
# Only data owners (with section access) can publish tables.
# Published metadata is synced to the master catalog for discovery.

cat("\nPublishing to master catalog...\n")

# Publish selected tables (using public=TRUE in db_describe)
db_describe(
  schema = "trade",
  table = "imports",
  public = TRUE  # Syncs to master discovery catalog
)
cat("✓ trade.imports published\n")

db_describe(
  schema = "trade",
  table = "exports",
  public = TRUE
)
cat("✓ trade.exports published\n")

db_describe(
  schema = "reference",
  table = "countries",
  public = TRUE
)
cat("✓ reference.countries published\n")

# Labour and health tables remain private
cat("  labour.employment - private (internal only)\n")
cat("  health.hospitals - private (internal only)\n")

# Show public catalog contents
cat("\nPublic tables in master catalog:\n")
public_tables <- db_list_public()
if (nrow(public_tables) > 0) {
  print(public_tables[, c("section", "schema", "table", "description")])
} else {
  cat("  (none)\n")
}

# Show registered sections
cat("\nRegistered sections:\n")
print(db_list_registered_sections())

# =============================================================================
# Show what we created
# =============================================================================

cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("DUCKLAKE TEST DATA READY\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n\n")

cat("Current section: ", db_current_section(), "\n")

cat("Schemas and tables:\n")
for (sch in db_list_schemas()) {
  if (startsWith(sch, "_")) next  # skip metadata schema
  tables <- db_list_tables(sch)
  cat(sprintf("  %s (%d tables)\n", sch, length(tables)))
  for (tbl in tables) {
    cat(sprintf("    - %s\n", tbl))
  }
}

cat("\nSnapshots:\n")
print(db_snapshots())

cat("\nConnection status:\n")
db_status()

# =============================================================================
# Launch browser
# =============================================================================

cat("\n")
cat("Launching browser...\n")
cat("Try the 'Public Catalog' tab to see published tables and sections!\n")
cat("(Close the browser window to return to R)\n\n")

db_browser()

# Cleanup
cat("\nCleaning up...\n")
db_disconnect()
options(datapond.master_catalog = NULL)
unlink(lake_path, recursive = TRUE)
cat("Done!\n")
