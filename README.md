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

The project description reports more than 2.2 million UK crime records enriched with police workforce information.

> **[ACTION REQUIRED]** Add the exact source URLs, publishers, date ranges, download dates and licences for both the crime records and police-strength data. Confirm whether raw files may be redistributed. If not, remove them and provide acquisition instructions or scripts.

The repository’s MIT licence applies to original code only. It does not relicense public-sector datasets or third-party assets.

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

Create local input and output paths. Do not commit machine-specific paths or database credentials.

> **[ACTION REQUIRED]** Refactor any hard-coded file paths and credentials into environment variables or an ignored configuration file. Add a `.env.example` containing names only, never secrets.

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
