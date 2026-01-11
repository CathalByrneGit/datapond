# browser_demo_ducklake.R
# ========================
# Interactive demo of db_browser() with sample DuckLake data
# Demonstrates path-based access control with schema paths
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
dir.create(lake_path, recursive = TRUE)

cat("Setting up DuckLake at:", lake_path, "\n\n")

# =============================================================================
# Set up DuckLake catalog
# =============================================================================
# Single catalog file with schemas that have custom data paths.
# In production, folder ACLs on each schema path control access.

catalog_path <- file.path(lake_path, "catalog.sqlite")
data_path <- file.path(lake_path, "data")
dir.create(data_path, recursive = TRUE, showWarnings = FALSE)

# Connect to DuckLake
db_lake_connect(
  catalog = "demo",
  catalog_type = "sqlite",
  metadata_path = catalog_path,
  data_path = data_path
)
cat("DuckLake catalog created\n")

# =============================================================================
# Create schemas with custom paths (for access control)
# =============================================================================
# Each schema has its own data folder. In production, you'd set folder ACLs
# on each path to control who can read/write to that schema.

trade_path <- file.path(data_path, "trade")
labour_path <- file.path(data_path, "labour")
health_path <- file.path(data_path, "health")
shared_path <- file.path(data_path, "shared")

# Create directories
dir.create(trade_path, recursive = TRUE, showWarnings = FALSE)
dir.create(labour_path, recursive = TRUE, showWarnings = FALSE)
dir.create(health_path, recursive = TRUE, showWarnings = FALSE)
dir.create(shared_path, recursive = TRUE, showWarnings = FALSE)

# Create schemas with paths
db_create_schema("trade", path = trade_path)
db_create_schema("labour", path = labour_path)
db_create_schema("health", path = health_path)
db_create_schema("reference", path = shared_path)

cat("Created schemas with custom paths\n")
cat("  trade:     ", trade_path, "\n")
cat("  labour:    ", labour_path, "\n")
cat("  health:    ", health_path, "\n")
cat("  reference: ", shared_path, "\n\n")

# =============================================================================
# Create sample tables
# =============================================================================

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
cat("Created trade.imports\n")

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
cat("Created trade.exports\n")

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
cat("Created labour.employment\n")

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
cat("Created health.hospitals\n")

# reference.countries
countries <- data.frame(
  code = c("DE", "FR", "UK", "US", "CN", "JP", "IE"),
  name = c("Germany", "France", "United Kingdom", "United States", "China", "Japan", "Ireland"),
  region = c("Europe", "Europe", "Europe", "Americas", "Asia", "Asia", "Europe")
)

db_lake_write(countries, schema = "reference", table = "countries",
              commit_author = "demo", commit_message = "Initial load")
cat("Created reference.countries\n")

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

cat("Documentation added\n")

# =============================================================================
# Show what we created
# =============================================================================

cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("DUCKLAKE TEST DATA READY\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n\n")

cat("Schemas and their data paths:\n")
for (sch in db_list_schemas()) {
  if (startsWith(sch, "_")) next  # skip metadata schema
  path <- db_get_schema_path(sch)
  tables <- db_list_tables(sch)
  cat(sprintf("  %s (%d tables)\n", sch, length(tables)))
  if (!is.null(path)) {
    cat(sprintf("    path: %s\n", path))
  }
  for (tbl in tables) {
    cat(sprintf("    - %s\n", tbl))
  }
}

cat("\nSnapshots:\n")
print(db_snapshots())

cat("\nConnection status:\n")
db_status()

# =============================================================================
# Demonstrate access control concept
# =============================================================================

cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("ACCESS CONTROL VIA SCHEMA PATHS\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n\n")

cat("In production, you would set folder ACLs on each schema's data path:\n\n")
cat("  Schema      Path                          ACL Example\n")
cat("  ---------   ---------------------------   ---------------------------\n")
cat("  trade       ", trade_path, "    Trade Team: read/write\n")
cat("  labour      ", labour_path, "   Labour Team: read/write\n")
cat("  health      ", health_path, "   Health Team: read/write\n")
cat("  reference   ", shared_path, "   Everyone: read\n")
cat("\n")
cat("This enables fine-grained access control using familiar folder permissions.\n")

# =============================================================================
# Launch browser
# =============================================================================

cat("\n")
cat("Launching browser...\n")
cat("Check the 'Public Catalog' tab for information about access control.\n")
cat("(Close the browser window to return to R)\n\n")

db_browser()

# Cleanup
cat("\nCleaning up...\n")
db_disconnect()
unlink(lake_path, recursive = TRUE)
cat("Done!\n")
