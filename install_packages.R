required_packages <- c(
  "data.table",
  "dplyr",
  "tidyverse",
  "readr",
  "lubridate",
  "DBI",
  "RPostgres",
  "odbc",
  "RODBC"
)

missing_packages <- required_packages[
  !required_packages %in% rownames(installed.packages())
]

if (length(missing_packages) > 0) {
  install.packages(
    missing_packages,
    repos = "https://cloud.r-project.org"
  )
}

message("All required R packages are installed.")
