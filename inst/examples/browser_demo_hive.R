# browser_demo_hive.R
# =====================
# Interactive demo of db_browser() with sample hive-partitioned data
#
# Run with:
#   source(system.file("examples", "browser_demo_hive.R", package = "csolake"))
#
# Or use the helper:
#   csolake::run_example("browser_demo_hive")

library(csolake)

# Create a temp directory for our test data lake
lake_path <- file.path(tempdir(), "test_lake")
if (dir.exists(lake_path)) unlink(lake_path, recursive = TRUE)
dir.create(lake_path)

cat("Setting up test data lake at:", lake_path, "\n\n")

# Connect
db_connect(path = lake_path)

# =============================================================================
# Create sample datasets
# =============================================================================

# Trade/Imports - monthly import data
imports <- data.frame(

year = rep(2023:2024, each = 12),
month = rep(1:12, 2),
country = sample(c("DE", "FR", "UK", "US", "CN", "JP"), 24, replace = TRUE),
commodity = sample(c("Machinery", "Chemicals", "Food", "Textiles", "Electronics"), 24, replace = TRUE),
value = round(runif(24, 100, 10000), 2),
quantity = sample(100:5000, 24)
)

db_hive_write(imports, "Trade", "Imports", partition_by = c("year", "month"))
cat("✓ Created Trade/Imports\n")

# Trade/Exports - monthly export data
exports <- data.frame(
year = rep(2023:2024, each = 12),
month = rep(1:12, 2),
country = sample(c("DE", "FR", "UK", "US", "CN", "JP"), 24, replace = TRUE),
commodity = sample(c("Dairy", "Beef", "Pharma", "Software", "Machinery"), 24, replace = TRUE),
value = round(runif(24, 50, 8000), 2),
quantity = sample(50:3000, 24)
)

db_hive_write(exports, "Trade", "Exports", partition_by = c("year", "month"))
cat("✓ Created Trade/Exports\n")

# Labour/Employment - quarterly employment stats
employment <- data.frame(
year = rep(2022:2024, each = 4),
quarter = rep(1:4, 3),
sector = rep(c("Agriculture", "Manufacturing", "Services", "Public"), 3),
employed = sample(50000:500000, 12),
unemployed = sample(5000:50000, 12),
participation_rate = round(runif(12, 0.55, 0.75), 3)
)

db_hive_write(employment, "Labour", "Employment", partition_by = "year")
cat("✓ Created Labour/Employment\n")

# Labour/Earnings - average weekly earnings
earnings <- data.frame(
year = rep(2022:2024, each = 4),
quarter = rep(1:4, 3),
sector = rep(c("Agriculture", "Manufacturing", "Services", "Public"), 3),
avg_weekly_earnings = round(runif(12, 600, 1200), 2),
median_weekly_earnings = round(runif(12, 500, 1000), 2)
)

db_hive_write(earnings, "Labour", "Earnings", partition_by = "year")
cat("✓ Created Labour/Earnings\n")

# Health/Hospitals - hospital stats
hospitals <- data.frame(
hospital_id = paste0("H", sprintf("%03d", 1:20)),
name = paste("Hospital", LETTERS[1:20]),
county = sample(c("Dublin", "Cork", "Galway", "Limerick", "Waterford"), 20, replace = TRUE),
beds = sample(50:500, 20),
staff = sample(100:2000, 20),
type = sample(c("General", "Specialist", "Teaching"), 20, replace = TRUE)
)

db_hive_write(hospitals, "Health", "Hospitals")
cat("✓ Created Health/Hospitals\n")

# Reference/Countries - lookup table
countries <- data.frame(
code = c("DE", "FR", "UK", "US", "CN", "JP", "IE"),
name = c("Germany", "France", "United Kingdom", "United States", "China", "Japan", "Ireland"),
region = c("Europe", "Europe", "Europe", "Americas", "Asia", "Asia", "Europe")
)

db_hive_write(countries, "Reference", "Countries")
cat("✓ Created Reference/Countries\n")

# =============================================================================
# Add documentation
# =============================================================================

cat("\nAdding documentation...\n")

# Trade/Imports
db_describe(
section = "Trade",
dataset = "Imports",
description = "Monthly import values by country and commodity. Source: CSO Trade Statistics.",
owner = "Trade Section",
tags = c("trade", "monthly", "official", "imports")
)

db_describe_column("Trade", "Imports", column = "value",
                   description = "Import value",
                   units = "EUR (thousands)")
db_describe_column("Trade", "Imports", column = "quantity",
                   description = "Quantity imported",
                   units = "tonnes")
db_describe_column("Trade", "Imports", column = "country",
                   description = "ISO 2-letter country code of origin")

# Trade/Exports
db_describe(
section = "Trade",
dataset = "Exports",
description = "Monthly export values by country and commodity. Source: CSO Trade Statistics.",
owner = "Trade Section",
tags = c("trade", "monthly", "official", "exports")
)

db_describe_column("Trade", "Exports", column = "value",
                   description = "Export value",
                   units = "EUR (thousands)")

# Labour/Employment
db_describe(
section = "Labour",
dataset = "Employment",
description = "Quarterly employment statistics by sector from the Labour Force Survey.",
owner = "Labour Market Section",
tags = c("labour", "quarterly", "LFS", "employment")
)

db_describe_column("Labour", "Employment", column = "employed",
                   description = "Number of persons employed",
                   units = "persons")
db_describe_column("Labour", "Employment", column = "participation_rate",
                   description = "Labour force participation rate",
                   units = "proportion (0-1)")

# Labour/Earnings
db_describe(
section = "Labour",
dataset = "Earnings",
description = "Average and median weekly earnings by sector.",
owner = "Labour Market Section",
tags = c("labour", "quarterly", "earnings", "wages")
)

# Health/Hospitals
db_describe(
section = "Health",
dataset = "Hospitals",
description = "Hospital reference data including capacity and staffing.",
owner = "Health Section",
tags = c("health", "reference", "hospitals")
)

db_describe_column("Health", "Hospitals", column = "beds",
                   description = "Total bed capacity")
db_describe_column("Health", "Hospitals", column = "staff",
                   description = "Total staff count",
                   units = "FTE")

# Reference/Countries
db_describe(
section = "Reference",
dataset = "Countries",
description = "Country code lookup table (ISO 3166-1 alpha-2).",
owner = "Data Governance",
tags = c("reference", "lookup", "countries")
)

cat("✓ Documentation added\n")

# =============================================================================
# Show what we created
# =============================================================================

cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("TEST DATA LAKE READY\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n\n")

cat("Sections:\n")
for (sec in db_list_sections()) {
datasets <- db_list_datasets(sec)
cat(sprintf("  %s/ (%d datasets)\n", sec, length(datasets)))
for (ds in datasets) {
  cat(sprintf("    - %s\n", ds))
}
}

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
