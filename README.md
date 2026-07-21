# Crime Data Warehouse & Analytics Pipeline

[![R](https://img.shields.io/badge/R-ETL-276DC3)](https://www.r-project.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-warehouse-4169E1)](https://www.postgresql.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-live-FF4B4B)](https://crime-data-warehouse-project-werssygoztfug6fijvbfew.streamlit.app/)
[![License: MIT](https://img.shields.io/badge/Code%20License-MIT-green.svg)](LICENSE)

An end-to-end public-data engineering project that uses R for extraction and transformation, PostgreSQL and SQL for dimensional warehousing, and Streamlit for interactive crime analytics.

## Problem addressed

Public crime data is distributed across files, periods and geographic levels. This project demonstrates how those records can be cleaned, enriched and organised into an analytical warehouse supporting repeatable trend, location, outcome and staffing analysis.

## Public demonstration

- Live app: *[CMP APP](https://crime-data-warehouse-project-werssygoztfug6fijvbfew.streamlit.app/)*
- Streamlit entry point: `ccc_app.py`
- ETL script: `ETL_Process_Crime.R`
- Warehouse SQL: `Crime_df_warehouse_code.sql`

The deployment is an analytical demonstration. It does not provide operational policing recommendations or establish causal relationships between police staffing and crime.

## Data provenance

This project combines publicly available street-level crime records with police workforce statistics.

### Crime records

- **Publisher:** UK Home Office / data.police.uk
- **Dataset:** Street-level crime and latest outcome data
- **Source:** https://data.police.uk/data/
- **Geographic coverage:** London
- **Period used:** 2020 - 2025
- **Downloaded:** 2025
- **Licence:** Open Government Licence v3.0
- **Licence information:** https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

The source provides CSV files containing street-level crime and outcome information by police force and Lower Layer Super Output Area (LSOA). The project combines and transforms the selected monthly files for analytical warehousing.

### Police workforce data

- **Publisher:** Home Office
- **Dataset:** Police workforce open data tables
- **Source:** https://www.gov.uk/government/statistical-data-sets/police-workforce-open-data-tables
- **Measures used:** Police officer strength, police staff strength and Police Community Support Officer strength
- **Period used:** 2020 - 2025
- **Downloaded:** 2025
- **Licence:** Open Government Licence v3.0
- **Licence information:** https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

The workforce data was standardised to a monthly date field and joined to the crime records to support descriptive comparisons between recorded crime volume and workforce measures.

### Processing and redistribution

The ETL workflow standardises dates and column names, handles incomplete records, derives time attributes and joins the selected crime and workforce data. The resulting warehouse supports analysis by time, LSOA, location, crime type, outcome and workforce measure.

The source data remains subject to the Open Government Licence and is not covered by this repository's MIT licence. The MIT licence applies only to original code authored for this project.

Users should obtain the latest source files from the official publishers. Any source or transformed data redistributed through this repository must retain the required attribution and comply with the applicable Open Government Licence terms.

This project is an analytical demonstration. Observed associations between crime records and workforce measures do not establish that staffing levels cause changes in recorded crime.

## Architecture

```text
Monthly crime files + police-strength data
                    |
                    v
R ETL: ingestion, validation, cleaning, date standardisation and enrichment
                    |
                    v
PostgreSQL staging table
                    |
                    v
Kimball-style dimensions and fact tables
                    |
                    v
SQL analytics and Streamlit visualisation
```

## Technical components

### ETL

`ETL_Process_Crime.R` combines monthly files, standardises fields, handles missing data, derives date attributes and joins contextual police-strength data.

### Warehouse

`Crime_df_warehouse_code.sql` builds analytical dimensions and facts for crime type, geography, location, outcomes and time-based reporting.

### Application

`ccc_app.py` exposes selected analytical outputs through a browser-based Streamlit interface.

## Run locally

### 1. Prepare the environment

- Install R and the packages documented in the ETL script.
- Install PostgreSQL.
- Install Python 3.10 or later for the Streamlit layer.

### 2. Configure data sources


The ETL reads local file locations and PostgreSQL connection settings from environment variables. Copy `.env.example` and configure the values for your local environment.

Required variables:

```text
CRIME_DATA_DIR
POLICE_DATA_FILE
DB_NAME
DB_HOST
DB_PORT
DB_USER
DB_PASSWORD
```

### 3. Run the ETL

```bash
Rscript ETL_Process_Crime.R
```

### 4. Build the warehouse

Run `Crime_df_warehouse_code.sql` against a development PostgreSQL database after reviewing its schema and connection assumptions.

### 5. Run the application

```bash
python -m venv .venv
pip install -r requirements.txt
streamlit run ccc_app.py
```

> **[ACTION REQUIRED]** Generate and test the R dependency manifest and Python `requirements.txt` from a clean environment.

## Reproducibility checks

- Record the source-data date range and row counts before and after each transformation.
- Log rejected or incomplete rows without exposing sensitive values.
- Validate primary/foreign-key expectations before loading facts.
- Add tests for date parsing, categorisation and duplicate handling.
- Document the grain of every fact table.

## Intended use and limitations

- Educational and portfolio demonstration of ETL, dimensional modelling and public-data analytics.
- Aggregated patterns do not establish causation.
- Police-strength comparisons may be affected by population, reporting, deployment and geographic differences.
- Geographic and outcome fields inherit limitations from the source datasets.
- Do not use the application to identify or profile individuals.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md). Contributions should preserve data provenance, reproducibility and privacy.

## Security and privacy

See [SECURITY.md](SECURITY.md). Never commit database passwords, connection strings, private records or precise personal identifiers.

## Citation

Citation metadata is provided in [CITATION.cff](CITATION.cff).

## Licence

Original code is available under the [MIT License](LICENSE). Source datasets and third-party assets remain governed by their original terms.
