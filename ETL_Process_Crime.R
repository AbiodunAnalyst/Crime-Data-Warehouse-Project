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
# Define a function to read and merge multiple CSV files using data.table
merge_csv_files <- function(mypath) {
  # Get the list of file names
  filenames <- list.files(path = mypath, full.names = TRUE)
  
  # Read and merge CSV files using data.table's fread function
  merged_data <- rbindlist(lapply(filenames, fread))
  
  return(merged_data)
}

# Specify the directory containing CSV files
directory_path <- "PATH"

# Call the merge_csv_files function to merge CSV files from the directory
Crime_data_df <- merge_csv_files(directory_path)

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


# --------------------- Load into PostgreSQL ------------------------------

# Connection to the database
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "CrimeProject",
                 host = "-----",
                 port = ----,
                 user = "postgres",
                 password = ----) 

# Write to the database, overwriting existing table
dbWriteTable(con, "crime_df", Crime_df, overwrite = TRUE)



# Write to the database, appending data to existing table
#dbWriteTable(con, "Crime_df", Crime_df, append = TRUE)


# Write to the database with a different table name
#dbWriteTable(con, "New_Crime_df", Crime_df)


dbDisconnect(con)





























