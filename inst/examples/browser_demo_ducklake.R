# browser_demo_ducklake.R
# ========================
# Interactive demo of db_browser() with sample DuckLake data
#
# Run with:
#   source(system.file("examples", "browser_demo_ducklake.R", package = "csolake"))
#
# Or use the helper:
#   csolake::run_example("browser_demo_ducklake")

library(csolake)

# Create a temp directory for our test data lake
lake_path <- file.path(tempdir(), "test_ducklake")
if (dir.exists(lake_path)) unlink(lake_path, recursive = TRUE)
dir.create(lake_path)

cat("Setting up DuckLake at:", lake_path, "\n\n")

# Connect in DuckLake mode
db_lake_connect(
  catalog = "demo",
  metadata_path = file.path(lake_path, "catalog.ducklake"),
  data_path = lake_path
)

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
# Show what we created
# =============================================================================

cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("DUCKLAKE TEST DATA READY\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n\n")

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
cat("(Close the browser window to return to R)\n\n")

db_browser()

# Cleanup
cat("\nCleaning up...\n")
db_disconnect()
unlink(lake_path, recursive = TRUE)
cat("Done!\n")
