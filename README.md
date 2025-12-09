<h1 align="center">🏛 Crime Data Warehouse & Analytics Pipeline</h1>

<p align="center">
  <strong>R ETL • Dimensional Modelling • PostgreSQL Warehouse • Data Quality Engineering • BI-Ready Fact Tables</strong>
</p>

<p align="center">
  End-to-end data warehouse pipeline built for the UK Crime Dataset, featuring automated ETL in R, enriched data modelling, 
  and warehouse-ready fact/dimension tables powering advanced crime analytics. 
  Suitable for UK Global Talent Visa (Technical Path) evidence.
</p>

<p align="center">

  <img src="https://img.shields.io/badge/R-276DC3?style=for-the-badge&logo=r&logoColor=white" />
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Data%20Warehouse-Kimball-blue?style=for-the-badge" />
  <img src="https://img.shields.io/badge/ETL%20Pipeline-R%20Script-green?style=for-the-badge" />
  <img src="https://img.shields.io/badge/BI%20Ready-Power%20BI-yellow?style=for-the-badge&logo=powerbi" />

</p>

---

<p align="center">
  <em>This project demonstrates enterprise-level ETL design, dimensional modelling, and warehouse engineering for large-scale public safety analytics.</em>
</p>

---

# 📌 Project Overview

This project implements a **Crime Analytics Data Warehouse** using R for ETL and PostgreSQL for storage.  
The system ingests **over 2.2 million UK crime records**, enriches them with **police force strength data**, validates quality, and produces **clean staging data** for a Kimball-style warehouse schema.

The final dataset powers analytics on:

- Crime distribution by geography  
- Crime type trends over time  
- Crime outcomes and resolution rates  
- Relationship between crime and police staffing  

---

## 📦 End-to-End Crime Analytics Pipeline (ETL → Warehouse → BI)

---

             ┌────────────────────────────────────┐
             │      Raw Crime Data Files          │
             │  (Monthly CSVs from data portal)   │
             └───────────────────┬────────────────┘
                                 │
                                 ▼
             ┌────────────────────────────────────┐
             │     R ETL Pipeline (crime_df)      │
             │  - File ingestion & merging        │
             │  - Data cleaning & NA handling     │
             │  - Business rules (behavioural)    │
             │  - Date parsing (YYYY-MM)          │
             │  - Join with Police Strength data  │
             └───────────────────┬────────────────┘
                                 │
                                 ▼
             ┌────────────────────────────────────┐
             │     PostgreSQL Staging Table       │
             │              crime_df              │
             │  - Clean, enriched crime records   │
             │  - Ready for dimensional loading   │
             └───────────────────┬────────────────┘
                                 │
                                 ▼
             ┌────────────────────────────────────┐
             │   Data Warehouse (Star Schema)     │
             │  - dim_crime_type                  │
             │  - dim_lsoa                        │
             │  - dim_location                    │
             │  - dim_outcome                     │
             │  - dim_date                        │
             │  - fact_crime_num                  │
             │  - fact_crime_count                │
             │  - fact_crime_resolution           │
             │  - fact_crime_time                 │
             └───────────────────┬────────────────┘
                                 │
                                 ▼
             ┌────────────────────────────────────┐
             │   BI & Analytics Layer             │
             │  - Power BI / SQL reporting        │
             │  - Crime trend analysis            │
             │  - Hotspot & outcome insights      │
             │  - Staffing vs crime correlations  │
             └────────────────────────────────────┘




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
