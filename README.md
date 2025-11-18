# BDET-mobility: Spanish Population Mobility Analysis

**Project for Big Data Engineering Technologies**
<br>
**Authors:** Julia García & Carlos Torregrosa

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)
![Status](https://img.shields.io/badge/Methodology-Agile-orange)

## 📖 Overview

This repository contains an end-to-end data analysis pipeline that correlates human mobility patterns in Spain with socio-economic data. Using **DuckDB** for high-performance in-process SQL and **MITMA** (Ministry of Transport) open data, this project constructs a Medallion Architecture (Bronze $\to$ Silver $\to$ Gold) to identify infrastructure gaps and mobility trends.

This document serves as a record of the technical decisions made during the development process, simulating Agile methodology sprints.

---

## 🔄 Agile Development Log

### 1. Data Acquisition (Sprint 1)
**Challenge:** We initially located the data on the [MITMA Open Data portal](https://www.transportes.gob.es/ministerio/proyectos-singulares/estudios-de-movilidad-con-big-data/opendata-movilidad). However, manually downloading every monthly dataset was infeasible due to physical storage constraints and download times (approx. 5GB per file).

**Solution:** We utilized the `pyspainmobility` Python package provided by the ministry to streamline extraction.

**Library Configuration Used:**
According to the [documentation](https://pyspainmobility.github.io/pySpainMobility/reference/mobility.html), the key parameters for the `Mobility` class are:

* `version` (int): Data version (Default: 2). Version 1 covers 2020-2021; Version 2 covers 2022 onwards.
* `zones` (str): Geographic granularity. Options: `districts`, `municipalities` (default), `large_urban_areas` (GAU).
* `start_date` (str): Required format `YYYY-MM-DD`.
* `end_date` (str): Optional. Defaults to `start_date` if not specified.
* `output_directory` (str): Destination for raw data and processed parquet files.
* `use_dask` (bool): Option to use Dask for large dataset processing.

### 2. Data Exploration (Sprint 2)
**Strategy:** We analyzed the official [examples folder](https://github.com/pyspainmobility/pySpainMobility/tree/main/examples) from the repository. Specifically, we adapted the logic from `examples/01-madrid.ipynb` to apply it to the **Valencia** region.

**Initial Finding:**
* The calculated mean distance of population mobility in the sample period is **70.09 km**.

---

## 🏗️ Data Pipeline Architecture

The project is structured around a **DuckDB Medallion Architecture**:

### 🥉 Bronze Layer (Raw Ingestion)
* **Mobility Data:** Ingested via `pyspainmobility` for Districts, Municipalities, and GAUs.
* **Socio-economic Data:** Raw Excel ingestion of Population (`poblaciones.xlsx`) and Income (`rentas.xlsx`) from INE.

### 🥈 Silver Layer (Cleaning & Integration)
* **Identity Resolution:** Mapping table (`rel_muni`) created to link MITMA transport codes with INE census codes.
* **GAU Parsing:** Logic to resolve "Zone GAU [City]" strings to actual municipality names.
* **Enrichment:** Joins trip data with population and average rent metrics.

### 🥇 Gold Layer (Business Aggregates)
* **Infrastructure Index:** Implementation of a Gravity Model ($Pop_A \times Pop_B / Dist^2$) to compare theoretical demand vs. actual trips.
* **Provincial Rollup:** Aggregation of municipal flows to analyze inter-provincial isolation (e.g., identifying provinces with the lowest interaction with Valencia).

---

## 🛠️ Tech Stack

* **Language:** Python 3.x
* **Query Engine:** [DuckDB](https://duckdb.org/) (In-process SQL OLAP)
* **Data Access:** `pyspainmobility`
* **Data Manipulation:** Pandas
* **Visualization:** Seaborn / Matplotlib

## 🚀 Getting Started

### Prerequisites
```bash
pip install pandas duckdb pyspainmobility seaborn matplotlib openpyxl
