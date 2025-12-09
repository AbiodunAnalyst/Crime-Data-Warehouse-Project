## 🏛 Crime Analytics Data Warehouse (SQL)

This project implements a Kimball-style **data warehouse** for UK crime data using SQL.

Source data from a staging table (`Crime_df`) is transformed into:

### 🔹 Dimension Tables
- `dim_crime_type` – types of crime (e.g. burglary, robbery, violence)  
- `dim_LSOAName` – LSOA names and codes for geographic analysis  
- `dim_Location` – free-text location descriptions  
- `dim_Outcome` – last outcome category (e.g. "Under investigation", "No further action")  
- `dim_date` – full calendar date dimension (day, month, quarter, year, year_month)

### 🔹 Fact Tables
Each fact table represents a different analytical “lens” on crime:

- `fact_Crime_Num` – number of crimes by date, area, location, type and police strength  
- `fact_Crime_Count` – alternative crime count grain by date, LSOA, location and type  
- `fact_Resolution` – number of resolved crimes by outcome and crime type  
- `fact_occuring_Time` – crime counts by day of week and location

All fact tables use **foreign keys** back to the dimension tables (`dim_crime_type`, `dim_LSOAName`, `dim_Location`, `dim_date`, `dim_Outcome`), forming a classic star schema.

### 🧠 What this enables

Analysts and BI tools (e.g. Power BI, Tableau) can now easily answer questions like:

- How do crime volumes vary by **crime type and LSOA** over time?  
- Which areas show the highest **crime resolution rates** by outcome category?  
- On which **day of the week** do specific crime types peak?  
- Is there any relationship between **police strength** and recorded crime volume?

This project showcases my ability to design and implement a **dimensional model**, generate a **date dimension**, and build **SQL-based ETL** for a real analytics use case.
