# BDET-mobility: Spanish Population Mobility Analysis

**Project for Big Data Engineering Technologies**
<br>
**Authors:** Julia García & Carlos Torregrosa

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)
![DuckLake](https://img.shields.io/badge/Storage-DuckLake-green)

## Executive Summary
This repository contains a high-performance Data Lakehouse implementation designed to analyze human mobility patterns across Spain during 2023. By correlating massive mobility datasets from the Ministry of Transport (MITMA) with socio-economic indicators from the National Statistics Institute (INE), the project identifies infrastructure gaps and regional economic trends.

The solution is built on a **Medallion Architecture** orchestrated by **Apache Airflow**, utilizing **DuckDB** for vectorized SQL processing and **DuckLake** for ACID-compliant storage on **Amazon S3**.

---

## Data Pipeline Architecture

The pipeline transforms raw, heterogeneous data into actionable business intelligence through three specialized layers.

### 1. Bronze Layer: Raw Ingestion
* **High-Volume Ingestion:** Automated extraction of billions of trip records at District, Municipality, and GAU levels from S3.
* **Multimodal Sources:** Ingestion of structured CSV mobility data alongside spatial GeoJSON boundaries using the `ST_Read` extension.
* **Data Preservation:** Maintains an immutable "Source of Truth" by preserving raw files in their original format.

### 2. Silver Layer: Standardization & Enrichment
* **Data Harmonization:** Implementation of schema enforcement, casting types, and normalizing temporal data (dates and working-day flags).
* **ID Resolution:** A complex mapping process that translates MITMA-specific zonification codes into official INE identifiers to ensure interoperability.
* **Spatial Engineering:** Creation of a unified spatial catalog (`lugares`) that calculates centroids and performs spatial joins to associate locations with their respective provinces.
* **Fact Centralization:** Consolidation of disparate sources into a multi-billion row integrated fact table featuring calculated metrics like `viajes_metros`.

### 3. Gold Layer: Advanced Analytics
* **Clustering Analysis (BQ1):** Unsupervised K-Means clustering is applied to hourly mobility vectors to categorize days based on their movement profiles.
* **Infrastructure Coverage (BQ2):** Identification of disconnected regions by comparing observed trip volumes against a theoretical **Gravity Model**.
* **Industrial Hub Identification (BQ3):** Algorithmic detection of industrial centers based on the statistical ratio of workday-to-weekend mobility volume.

---

## Technical Stack

* **Core Engine:** [DuckDB](https://duckdb.org/) for high-efficiency OLAP transformations.
* **Orchestration:** [Apache Airflow](https://airflow.apache.org/) leveraging Dynamic Task Mapping for scalable file processing.
* **Storage & Metadata:** **Amazon S3** for the data lake, with **PostgreSQL** managing the DuckLake metadata layer.
* **Visualization & Reporting:**
    * **KeplerGL:** Generation of interactive geospatial flow maps.
    * **FPDF:** Automated generation of executive PDF reports.
    * **Scikit-Learn:** Machine learning pipelines for mobility pattern clustering.

---

## Documentation & Data Schema

For detailed technical specifications regarding table structures, record counts, and column definitions, please consult the following external resources:

* [cite_start]**Data Inventory & Dictionary:** Refer to the provided `datasheet.xlsx` for the complete catalog of Bronze, Silver, and Gold entities[cite: 1, 2].
* **System Architecture:** Consult the **Project Diagram PDF** for a visual representation of the Airflow DAG workflows and the DuckLake storage logic.
