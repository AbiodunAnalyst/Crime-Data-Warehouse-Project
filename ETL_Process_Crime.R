# ----------------------------- Libraries ----------------------------------

library(data.table)
library(dplyr)
library(tidyverse)
library(readr)
library(lubridate)
library(DBI)
library(RPostgres)
library(odbc)
library(RODBC)

# ------------------------ File Ingestion --------------------------
# Read the crime-data directory from an environment variable
crime_data_dir <- Sys.getenv("CRIME_DATA_DIR")

# Confirm that the environment variable has been configured
if (crime_data_dir == "") {
  stop(
    "CRIME_DATA_DIR has not been configured. ",
    "Set it to the directory containing the monthly crime CSV files."
  )
}

# Confirm that the configured directory exists
if (!dir.exists(crime_data_dir)) {
  stop("Crime-data directory not found: ", crime_data_dir)
}

# Find the monthly street-crime CSV files
crime_files <- list.files(
  path = crime_data_dir,
  pattern = "-street\\.csv$",
  full.names = TRUE
)

# Stop if no matching files were found
if (length(crime_files) == 0) {
  stop(
    "No street-crime CSV files were found in: ",
    crime_data_dir
  )
}

message("Crime files found: ", length(crime_files))

# Read and combine all monthly files
Crime_data_df <- data.table::rbindlist(
  lapply(crime_files, data.table::fread),
  use.names = TRUE,
  fill = TRUE
)

message(
  "Total crime records loaded: ",
  format(nrow(Crime_data_df), big.mark = ",")
)

# Display the structure without opening an interactive RStudio viewer
glimpse(Crime_data_df)

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

police_data_file <- Sys.getenv("POLICE_DATA_FILE")

if (police_data_file == "") {
  stop("POLICE_DATA_FILE has not been configured.")
}

if (!file.exists(police_data_file)) {
  stop("Police workforce file not found: ", police_data_file)
}

Police_data <- read.csv(
  police_data_file,
  stringsAsFactors = FALSE
)

message("Police strength data preview:")
glimpse(Police_data)

# --------------------- Missing Value Handling -----------------------------

message("NA counts per column:")
Police_data %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  print()

View(Police_data)

# Remove rows where all specified columns are NA or empty
Police_data <- Police_data %>%
  filter(!(is.na(Date) & is.na(Police.Officer.Strength) &
             is.na(Police.Staff.Strength) & is.na(PCSO.Strength)))



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


# --------------------- Load into PostgreSQL ------------------------------

# Connection to the database
db_name <- Sys.getenv("DB_NAME")
db_host <- Sys.getenv("DB_HOST")
db_port <- as.integer(Sys.getenv("DB_PORT", "5432"))
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")

required_db_values <- c(
  DB_NAME = db_name,
  DB_HOST = db_host,
  DB_USER = db_user,
  DB_PASSWORD = db_password
)

missing_db_values <- names(required_db_values)[required_db_values == ""]

if (length(missing_db_values) > 0) {
  stop(
    "Missing database environment variables: ",
    paste(missing_db_values, collapse = ", ")
  )
}

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname = db_name,
  host = db_host,
  port = db_port,
  user = db_user,
  password = db_password
)

# Write to the database, overwriting existing table
dbWriteTable(con, "crime_df", Crime_df, overwrite = TRUE)



# Write to the database, appending data to existing table
#dbWriteTable(con, "Crime_df", Crime_df, append = TRUE)


# Write to the database with a different table name
#dbWriteTable(con, "New_Crime_df", Crime_df)


dbDisconnect(con)







