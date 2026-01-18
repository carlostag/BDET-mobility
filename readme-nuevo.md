# BDET-mobility: Spanish Population Mobility Analysis

**Project for Big Data Engineering Technologies**
<br>
**Authors:** Julia García & Carlos Torregrosa

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)
![Airflow](https://img.scalable/badge/Orchestration-Apache_Airflow-red)
![DuckLake](https://img.shields.io/badge/Storage-DuckLake-green)

## Overview

This repository contains an end-to-end data lakehouse pipeline implemented using **Apache Airflow** and **DuckDB**. The project correlates massive mobility datasets from **MITMA** (2023) with socio-economic data from **INE**. [cite_start]By leveraging the **DuckLake** extension, we manage a scalable Medallion Architecture (Bronze $\to$ Silver $\to$ Gold) stored on **Amazon S3** with **Postgres** as the metadata layer[cite: 1].

## Data Pipeline Architecture

The workflow is orchestrated via Airflow DAGs (`mobility_bronze_silver`, `gold_bq_nuevo`), ensuring idempotency and efficient resource management through dynamic task mapping and S3 integration.

### Bronze Layer (Raw Ingestion)
* [cite_start]**Mobility Data:** Raw ingestion of trips for Distritos, Municipios, and GAUs from MITMA[cite: 1].
* [cite_start]**Socio-economic Data:** Raw ingestion of population and income data from INE[cite: 1].
* **Spatial Data:** Ingestion of GeoJSON files for provinces, municipalities, and districts using the `ST_Read` spatial extension.
* **Goal:** Preserve the data exactly as the source provided in `.csv.gz` or `.geojson` formats.

### Silver Layer (Cleaning & Integration)
* [cite_start]**Standardization:** All IDs are cast to Integers, names are trimmed, and dates are standardized[cite: 8, 9].
* [cite_start]**Fact Table Centralization:** The `viajes` table consolidates raw mobility sources into a single, cleaned source of truth, converting MITMA IDs to official INE IDs via a relationship lookup table[cite: 10].
* [cite_start]**Spatial Enrichment:** The `lugares` table unifies multiple administrative levels (municipio, distrito, gau) into a single catalog featuring centroids (latitude/longitude) and provincial associations[cite: 9].
* [cite_start]**Temporal Enrichment:** A standardized `calendario` table provides day-of-week and working-day (laborable) boolean flags[cite: 8].

### Gold Layer (Business Aggregates & Analytics)
* **BQ1 — Day Type Clustering:** Uses K-Means clustering to categorize days into three clusters based on their hourly mobility profiles, identifying distinct "types of days".
* **BQ2 — Gravity Mismatch:** Identifies infrastructure gaps by comparing real trips against a theoretical Gravity Model: $$(Pop_{origin} \times Rent_{destination}) / Distance^2$$.
* **BQ3 — Industrial Hubs:** Detects industrial zones by identifying municipalities with a high ratio of workday volume vs. weekend volume.
* [cite_start]**Goal:** Produce final business-ready tables and automated reports (PDFs and KeplerGL HTML maps) stored back in S3[cite: 1].

---

## Technical Inventory

### [cite_start]Table Registry [cite: 1]
| Layer | Table Name | Records | Origin | Description |
| :--- | :--- | :--- | :--- | :--- |
| **BRONZE** | `trips_distritos_2023` | 6,573,331,039 | MITMA | Raw ingestion of trip data. |
| **BRONZE** | `poblacion` | 8,145 | INE | Raw ingestion of population counts. |
| **SILVER** | `viajes` | 27,720,534,245 | CALCULATED | Standardized and cleaned version of trips. |
| **SILVER** | `lugares` | 6,642 | CALCULATED | Unified catalog of locations with geometry. |
| **GOLD** | `mobility_municipios_periodo` | 5,056,943,111 | CALCULATED | Business-ready table for municipality analysis. |

### [cite_start]Data Dictionary (Key Fields) [cite: 8, 9, 10]
| Table | Column | Type | Business Definition |
| :--- | :--- | :--- | :--- |
| `viajes` | `viajes_metros` | DOUBLE | Total meters of the trips made. |
| `calendario` | `tipo_dia` | BOOLEAN | Indicates if it was a working day or not. |
| `lugares` | `coordenadas` | GEOMETRY | Polygon or boundary of the location. |
| `poblacion` | `poblacion` | INTEGER | Demographic count for the specific area. |

---

## Tech Stack

* **Orchestration:** [Apache Airflow](https://airflow.apache.org/) (TaskGroups, Dynamic Task Mapping).
* **Query Engine:** [DuckDB](https://duckdb.org/) with `ducklake`, `spatial`, and `postgres` extensions.
* **Metadata & Storage:** Amazon S3 for data files and PostgreSQL for DuckLake metadata.
* **Analysis & Visualization:**
    * **KeplerGL:** For generating interactive HTML mobility maps.
    * **Scikit-Learn:** For K-Means clustering algorithms.
    * **Matplotlib/Seaborn:** For profile and trend charting.
    * **FPDF:** For automated PDF report generation.

## Getting Started

### Prerequisites
```bash
# Core requirements for the Airflow environment
pip install duckdb pandas matplotlib seaborn scikit-learn fpdf keplergl boto3
