# Enterprise Batch Data Integration Pipeline on Databricks

## Overview
This project implements a production-style **batch data engineering pipeline on Databricks** to integrate enterprise data from multiple source systems into analytics-ready datasets. The solution follows the **Medallion (Bronze–Silver–Gold) architecture** and simulates a real-world enterprise data integration scenario involving schema mismatches, data quality challenges, historical backfills, and daily incremental batch processing.

Source data is stored in **AWS S3 as an external data source**, ingested into Databricks, processed through staging and transformation layers, and finally modeled into curated dimension and fact tables for downstream analytics and reporting.

---

## Architecture Overview (Medallion Architecture)

The pipeline is designed using the **Medallion Architecture**, ensuring scalability, maintainability, and data quality.

### Bronze Layer
- Raw data ingestion from **AWS S3 (external storage)**
- Minimal transformation
- Preserves source structure for traceability

### Silver Layer
- Cleaned and standardized data
- Schema alignment and data quality checks
- Use of **staging tables** for daily batch processing
- Business rules applied

### Gold Layer
- Curated **dimension and fact tables**
- Optimized for analytics and BI consumption
- Supports reporting and dashboard use cases

---

## Data Ingestion & Processing Strategy

- **External Source**: AWS S3 (batch files)
- **Processing Type**: Scheduled batch processing (daily loads)
- **Staging Tables**: Created per batch to isolate raw data and apply validations
- **Load Strategies**:
  - Full load for initial historical data
  - Incremental load for daily data using merge logic
- **Orchestration**: Databricks Jobs

---

## Setup Layer

### setup/

This layer initializes the Databricks environment and shared components.

- **utilities.ipynb**  
  Contains reusable utility functions, configurations, and helper logic used across the pipeline.

- **dim_date_table_creation.ipynb**  
  Creates a Date Dimension table to support time-based analysis in fact tables.

- **setup_catalog_schemas.ipynb**  
  Sets up catalogs and schemas for Bronze, Silver, and Gold layers following Medallion architecture best practices.

---

## Dimension Data Processing

### dim_data_processing/

This layer focuses on transforming cleaned Silver-layer data into curated dimension tables.

- **customer_data.ipynb**  
  Processes customer data with standardization, deduplication, and schema alignment logic.

- **products_data.ipynb**  
  Transforms product-related data into analytics-ready dimension tables.

- **pricing_data.ipynb**  
  Cleans and structures pricing data for downstream fact processing and reporting.

All dimension tables are written to the **Gold layer** using Delta Lake.

---

## Fact Data Processing

### facts_data_processing/

This layer handles fact table creation and maintenance.

- **full_load_fact.ipynb**  
  Performs an initial full load of historical fact data.

- **incremental_load_fact.ipynb**  
  Processes daily incremental data using merge logic to update fact tables efficiently and accurately.

Fact tables are optimized for analytical and reporting use cases.

---

## Key Features

- Enterprise-style batch pipeline design
- External data ingestion from AWS S3
- Medallion (Bronze–Silver–Gold) architecture
- Staging table strategy for daily batch processing
- Full and incremental load handling
- Delta Lake–based merge operations
- Modular and reusable notebook structure
- Analytics-ready dimension and fact models

---

## Tools & Technologies

- Databricks  
- Apache Spark (Spark SQL)  
- Delta Lake  
- AWS S3  
- Databricks Jobs  
- Git  

---

## Outcome

The pipeline delivers reliable, analytics-ready datasets curated in the Gold layer and orchestrated using Databricks Jobs for scheduled batch execution. The processed data is consumed through Databricks Dashboards for reporting and leveraged via Genie to generate quick business insights and validate analytical queries. This project demonstrates real-world data engineering practices including external data ingestion, batch processing, orchestration, and analytics enablement on Databricks.

---

## Notes

This project is intended for learning and demonstration purposes and reflects patterns commonly used in large-scale enterprise data engineering environments.
