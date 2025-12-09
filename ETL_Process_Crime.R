library(data.table)
library(dplyr)
library(tidyverse)
library(sparklyr)

########################### LOADING THE DATA #######################################

# Define a function to read and merge multiple CSV files using data.table
merge_csv_files <- function(mypath) {
  # Get the list of file names
  filenames <- list.files(path = mypath, full.names = TRUE)
  
  # Read and merge CSV files using data.table's fread function
  merged_data <- rbindlist(lapply(filenames, fread))
  
  return(merged_data)
}

# Specify the directory containing CSV files
directory_path <- "D:/Abiodun/Path/DATA WAREHOUSE DATA/Crime_data/Data_Source"

# Call the merge_csv_files function to merge CSV files from the directory
Crime_data_df <- merge_csv_files(directory_path)


######################## Checking data Features #####################################################
# display table variable name
variable_names <- colnames(Crime_data_df)
print(variable_names)

# checking variables characteristics
glimpse(Crime_data_df)

dim(Crime_data_df)

# Select required variables for the analysis
Crime_data <- Crime_data_df %>%
  select("Crime ID", "Month", "Reported by", "Falls within", "Longitude", "Latitude", "Location", "LSOA code", 
         "LSOA name", "Crime type",  "Last outcome category")


nrow(Crime_data)

# Collect the result to R as an R DataFrame
#Crime_data <- collect(Crime_data)

# Number of column | Number of row
# -------------------------------------
#     2241522      |     11


##################### Check for Missing Value #####################################

na_counts_for_all_var <- Crime_data %>%
  summarize(across(everything(), list(na_count = ~sum(as.numeric(is.na(.)))))) %>% 
  show()


# Filter rows where multiple columns have missing values
filtered_data <- Crime_data %>% 
  filter(is.na(Longitude) & is.na(Latitude))

#view(filtered_data)

# Checking number of rows affected due to missing value
filtered_out_row <- nrow(filtered_data)
print(filtered_out_row)

# Remove filtered rows from the original data frame
Crime_data <- anti_join(Crime_data, filtered_data)

# checking number of row after deleting missing rows
Crime_data_num <- nrow(Crime_data)
print(Crime_data_num)

# Check if number remove plus remain equal total data
Total_Crime_data <- Crime_data_num + filtered_out_row
print(Total_Crime_data)

########### Checking for empty space ######################################

# Count empty strings across character variables
empty_counts_for_all_var  <- Crime_data %>%
  summarize(across(where(is.character), list(empty_count = ~sum(as.numeric(. == ""))))) %>% 
  show()

# Set last outcome category to Behavioral issues when crime type is Anti-social behaviour

Crime_data <- Crime_data %>%
  mutate(
    `Last outcome category` = if_else(`Crime type` == "Anti-social behaviour", 
                                      "Behavioral issues", 
                                      `Last outcome category`)
  )

# Rename the date column
Crime_data <- Crime_data %>%
  rename(Date = Month)


# Extract year and month components using substr() function
Crime_data <- Crime_data %>%
  mutate(Year = as.numeric(substr(Date, 1, 4)),  # Extract year (first 4 characters)
         Month = as.numeric(substr(Date, 6, 7)))  # Extract month (characters 6 and 7)


# Convert the Date column to Date type
Crime_data <- Crime_data %>%
  mutate(Date = as.Date(paste0(Date, "-01")))  %>%
  mutate(Date = format(Date, "%Y-%m"))


Crime_data <- Crime_data %>%
  rename(
    LSOA_code = `LSOA code`, 
    LSOA_name = `LSOA name`, 
    Crime_type = `Crime type`, 
    Reported_by = `Reported by`, 
    Falls_within = `Falls within`,
    Last_outcome_category = `Last outcome category`, 
    Crime_ID = `Crime ID`
  )


# Select required variables for the analysis
Crime_data <- Crime_data %>%
  select("Date", "Year", "Month", "LSOA_code", "LSOA_name", "Location", "Longitude", "Latitude", "Crime_type", "Last_outcome_category")

# Print the first few rows to verify
View(head(Crime_data))



############## Secondary data set ############################################

# File path to your CSV file
file_path <- "D:/Abiodun/Path/DATA WAREHOUSE DATA/Crime_data/Police_Force_Strength/Police_Force_Strength.csv"

# Import the CSV file into a data frame
Police_data <- read.csv(file_path)

# Print the first few rows of the data frame
View(head(Police_data))

# Checking the characters of each variables
glimpse(Police_data)

# Convert the date to the desired format
Police_data <- Police_data %>%
  mutate(
    # Convert the 'Date' column to Date object with original format
    Date = as.Date(Date, format = "%m/%d/%Y"),
    # Format the 'Date' column as "YYYY-MM"
    Date = format(Date, "%Y-%m")
  )
  
# Print 
View(head(Police_data))

# Joining primary and secondary data
Crime_df <- left_join(Crime_data,Police_data, by = "Date")

Crime_df <- Crime_df %>%
  rename(
    Police_Officer_Strength = Police.Officer.Strength, 
    Police_Staff_Strength  = Police.Staff.Strength,  
    PCSO_Strength  = PCSO.Strength 
  )



# Print
View(head(Crime_df))

glimpse(Crime_df)

# Save dataframe as CSV file
write.csv(Crime_df, file = "D:/Abiodun/Path/DATA WAREHOUSE DATA/final_data/Crime_df.csv", row.names = FALSE)

############### Connection to Posgresql ######################################

install.packages("RPostgres")

library(odbc)
library(RODBC)
library(DBI)
library(RPostgres)



# Connection to the database
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "CrimeProject",
                 host = "localhost",
                 port = 5432,
                 user = "postgres",
                 password = "marvel")  # Password should be enclosed in quotes

# Write to the database, overwriting existing table
dbWriteTable(con, "crime_df", Crime_df, overwrite = TRUE)



# Write to the database, appending data to existing table
#dbWriteTable(con, "Crime_df", Crime_df, append = TRUE)


# Write to the database with a different table name
#dbWriteTable(con, "New_Crime_df", Crime_df)


dbDisconnect(con)





















