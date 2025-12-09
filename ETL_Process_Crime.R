# ----------------------------- Libraries ----------------------------------

library(data.table)
library(dplyr)
library(tidyverse)
library(readr)
library(lubridate)
library(DBI)
library(RPostgres)


# -------------------------- Configuration ---------------------------------

# ❗ Update these paths/db settings for your environment

CRIME_DATA_DIR   <- "D:/Abiodun/Path/DATA WAREHOUSE DATA/Crime_data/Data_Source"
POLICE_FILE_PATH <- "D:/Abiodun/Path/DATA WAREHOUSE DATA/Crime_data/Police_Force_Strength/Police_Force_Strength.csv"
OUTPUT_CSV_PATH  <- "D:/Abiodun/Path/DATA WAREHOUSE DATA/final_data/Crime_df.csv"

DB_NAME       <- "CrimeProject"
DB_HOST       <- "------"
DB_PORT       <- ----
DB_USER       <- "postgres"
DB_PASSWORD   <- "----"    
DB_TABLE_NAME <- "crime_df"

# ------------------------ File Ingestion --------------------------

merge_csv_files <- function(mypath) {
  filenames <- list.files(path = mypath, pattern = "\\.csv$", full.names = TRUE)
  
  if (length(filenames) == 0) {
    stop("No CSV files found in directory: ", mypath)
  }
  
  message("Found ", length(filenames), " file(s). Reading...")
  dt_list <- lapply(filenames, fread)
  merged_data <- rbindlist(dt_list, fill = TRUE)
  
  message("Merged rows: ", nrow(merged_data))
  return(merged_data)
}

# ------------------------- Ingest Crime Data ------------------------------

Crime_data_df <- merge_csv_files(CRIME_DATA_DIR)

message("Columns in raw crime data:")
print(colnames(Crime_data_df))
message("Structure of raw crime data:")
glimpse(Crime_data_df)

message("Raw crime data dimensions: ", paste(dim(Crime_data_df), collapse = " x "))

# -------------------- Select Relevant Crime Variables ---------------------

Crime_data <- Crime_data_df %>%
  select(
    `Crime ID`,
    Month,
    `Reported by`,
    `Falls within`,
    Longitude,
    Latitude,
    Location,
    `LSOA code`,
    `LSOA name`,
    `Crime type`,
    `Last outcome category`
  )

message("Rows after selecting relevant columns: ", nrow(Crime_data))

# --------------------- Missing Value Handling -----------------------------

# NA counts per column
message("NA counts per column:")
Crime_data %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  print()

# Filter rows where both coordinates are missing
rows_missing_coord <- Crime_data %>%
  filter(is.na(Longitude) & is.na(Latitude))

message("Rows with missing coordinates: ", nrow(rows_missing_coord))

# Remove those rows
Crime_data <- anti_join(Crime_data, rows_missing_coord)

message("Rows after removing missing coordinate rows: ", nrow(Crime_data))

# Sanity check: removed + remaining = original
total_check <- nrow(Crime_data) + nrow(rows_missing_coord)
message("Original row count check: ", total_check, " (should equal raw subset rows)")

# --------------------- Empty String Checks ---------------------

message("Empty string counts for character variables:")
Crime_data %>%
  summarise(across(where(is.character), ~ sum(. == ""))) %>%
  print()

# --------------------- Business Logic & Transformations -------------------

# Set last outcome category to "Behavioral issues" when crime type is "Anti-social behaviour"
Crime_data <- Crime_data %>%
  mutate(
    `Last outcome category` = if_else(
      `Crime type` == "Anti-social behaviour",
      "Behavioral issues",
      `Last outcome category`
    )
  )

# Rename Month -> Date
Crime_data <- Crime_data %>%
  rename(Date = Month)

# Extract Year and Month from "YYYY-MM" format
Crime_data <- Crime_data %>%
  mutate(
    Year  = as.numeric(substr(Date, 1, 4)),
    Month = as.numeric(substr(Date, 6, 7))
  )

# Convert Date string to Date type at first day of month, then standardise back to "YYYY-MM"
Crime_data <- Crime_data %>%
  mutate(
    Date = as.Date(paste0(Date, "-01")),  # convert "YYYY-MM" to Date
    Date = format(Date, "%Y-%m")          # keep standardised "YYYY-MM" string for joining
  )

# Standardise column names
Crime_data <- Crime_data %>%
  rename(
    LSOA_code            = `LSOA code`,
    LSOA_name            = `LSOA name`,
    Crime_type           = `Crime type`,
    Reported_by          = `Reported by`,
    Falls_within         = `Falls within`,
    Last_outcome_category = `Last outcome category`,
    Crime_ID             = `Crime ID`
  )

# Keep only variables required for warehouse staging
Crime_data <- Crime_data %>%
  select(
    Date,
    Year,
    Month,
    LSOA_code,
    LSOA_name,
    Location,
    Longitude,
    Latitude,
    Crime_type,
    Last_outcome_category
  )

message("Crime_data after cleaning and transformation:")
glimpse(Crime_data)

# --------------------- Load Police Strength Data --------------------------

Police_data <- read.csv(POLICE_FILE_PATH, stringsAsFactors = FALSE)

message("Police strength data preview:")
glimpse(Police_data)

Police_data <- Police_data %>%
  mutate(
    Date = as.Date(Date, format = "%m/%d/%Y"),
    Date = format(Date, "%Y-%m")
  )

message("Police strength data after Date transformation:")
glimpse(Police_data)

# --------------------- Join Crime & Police Data --------------------------

Crime_df <- left_join(Crime_data, Police_data, by = "Date")

Crime_df <- Crime_df %>%
  rename(
    Police_Officer_Strength = Police.Officer.Strength,
    Police_Staff_Strength   = Police.Staff.Strength,
    PCSO_Strength           = PCSO.Strength
  )

message("Final Crime_df structure:")
glimpse(Crime_df)

# --------------------- Export to CSV -------------------------------------

write.csv(Crime_df, file = OUTPUT_CSV_PATH, row.names = FALSE)
message("Crime_df written to: ", OUTPUT_CSV_PATH)

# --------------------- 12. Load into PostgreSQL ------------------------------

message("Connecting to PostgreSQL...")

con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = DB_NAME,
  host     = DB_HOST,
  port     = DB_PORT,
  user     = DB_USER,
  password = DB_PASSWORD
)

# Ensure disconnection on exit
on.exit({
  try(dbDisconnect(con), silent = TRUE)
}, add = TRUE)

message("Writing table '", DB_TABLE_NAME, "' to database '", DB_NAME, "'...")

dbWriteTable(
  conn      = con,
  name      = DB_TABLE_NAME,
  value     = Crime_df,
  overwrite = TRUE
)


























