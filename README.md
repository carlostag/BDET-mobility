# BDET-mobility: Spanish Population Mobility Analysis

**Project for Big Data Engineering Technologies**
<br>
**Authors:** Julia García & Carlos Torregrosa

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)
![Status](https://img.shields.io/badge/Methodology-Agile-orange)

## Overview

This repository contains an end-to-end data analysis pipeline that correlates human mobility patterns in Spain with socio-economic data. Using **DuckDB** for high-performance in-process SQL and **MITMA** (Ministry of Transport) open data, this project constructs a Medallion Architecture (Bronze $\to$ Silver $\to$ Gold) to identify infrastructure gaps and mobility trends.

This document serves as a record of the technical decisions made during the development process, simulating Agile methodology sprints.

---

## Key Insights & Findings (Capa Gold)

El análisis de la movilidad semanal (Noviembre 2023) reveló los siguientes patrones y deficiencias de infraestructura:

* **Deficiencia de Infraestructura (KPI)**: Las zonas con el **peor servicio de infraestructura** (menor `indice_cobertura`) son distritos de grandes ciudades y sus áreas metropolitanas. Esto indica que la demanda teórica de viajes supera ampliamente la capacidad de las rutas actuales.
    * El `indice_cobertura` más bajo (cercano a **0.19**) se encuentra en distritos de **Hospitalet de Llobregat** y **Santa Coloma de Gramenet**, indicando que su infraestructura no absorbe el potencial de movilidad de su población.
* **Movilidad Dominante:** Los trayectos más concurridos son abrumadoramente **internos** (intra-municipal), con los pares Madrid $\leftrightarrow$ Madrid y Barcelona $\leftrightarrow$ Barcelona registrando el volumen de viajes más alto.
* **Flujos Inter-Metropolitanos:** Las rutas más fuertes entre grandes urbes (excluyendo viajes internos) son las que conectan **suburbios dormitorio** con el centro, como **Hospitalet de Llobregat $\leftrightarrow$ Barcelona** y **Getafe/Leganés $\leftrightarrow$ Madrid**.
* **Aislamiento Geográfico:** Las provincias con la **menor conectividad** interprovincial (menor `viajes_totales` salientes/entrantes) son las ciudades autónomas de **Melilla** y **Ceuta**, principalmente con provincias peninsulares lejanas como **Guipúzcoa** o **Salamanca**.

---

## Steps Followed

### 1. Data Acquisition
**Challenge:** We initially located the data on the [MITMA Open Data portal](https://www.transportes.gob.es/ministerio/proyectos-singulares/estudios-de-movilidad-con-big-data/opendata-movilidad). However, manually downloading every monthly dataset was infeasible due to physical storage constraints and download times (approx. 5GB per file).

**Solution:** We utilized the `pyspainmobility` Python package provided by the ministry to streamline extraction.

**Library Configuration Used:**
* `version` (int): Data version (Default: 2). Version 1 covers 2020-2021; Version 2 covers 2022 onwards.
* `zones` (str): Geographic granularity. Options: `districts`, `municipalities` (default), `large_urban_areas` (GAU).
* `start_date` (str): Required format `YYYY-MM-DD`.

### 2. Data Exploration
**Strategy:** We analyzed the official [examples folder](https://github.com/pyspainmobility/pySpainMobility/tree/main/examples) from the repository. Specifically, we adapted the logic from `examples/01-madrid.ipynb` to apply it to the **Valencia** region.

**Initial Finding:**
* The calculated mean distance of population mobility in the sample period is **70.09 km**.

---

## Data Pipeline Architecture

The project is structured around a **DuckDB Medallion Architecture**:

### Bronze Layer (Raw Ingestion)
* **Mobility Data:** Raw files for Distritos, Municipios, and GAUs.
* **Socio-economic Data:** Raw ingestion of Population and Income from INE.
* **Goal:** **Preserve the data exactly as the source provided.**

### Silver Layer (Cleaning & Integration)
* **Consolidation:** Combines the three raw mobility sources, resolving data overlap (desduplicación).
* **Enrichment:** Joins trip data with population, income, and temporal features (`day_of_week`).
* **Goal:** **Provide a single, cleaned, and granular source of truth.**

### Gold Layer (Business Aggregates)
* **Modeling:** Calculates the **Infrastructure Coverage Index** (KPI) using the Gravity Model.
* **Strategic Aggregation:** Rolls up data to the Provincial level to measure macro-flows.
* **Goal:** **Produce final, aggregated answers (KPIs) for the business.**

---

## Repository Data Schema

| Capa | Nombre de la Tabla | Granularidad / Función | Campos Clave (Schema) |
| :--- | :--- | :--- | :--- |
| **BRONZE** | `viajes_..._bronze` (3 tablas) | Datos de viaje crudos e inmutables. | `date`, `hour`, `id_origin`, `id_destination`, `n_trips`, `trips_total_length_km` |
| **BRONZE** | `poblacion_bronze`, `renta_bronze` | Datos socioeconómicos crudos. | `code`, `name`, `population`/`rent` |
| **SILVER** | `silver_zone_metrics` | Catálogo de Dimensiones/Métricas limpias. | `zone_id`, `zone_name`, `population`, `avg_rent` |
| **SILVER** | `rel_muni`, `dim_provincias` | Tablas de Traducción/Lookup (GAU/INE y códigos provinciales). | `code_ine`/`code_mitma` (`rel_muni`); `code`/`name` (`dim_provincias`) |
| **SILVER** | **`silver_integrated_od`** | **Hecho Central Enriquecido.** | `date`, `hour`, `n_trips`, `day_of_week`, `origin_name`, **`origin_population`**, **`destination_rent`**, etc. |
| **GOLD** | `gold_analisis_infraestructura` | KPI de Eficiencia de Infraestructura (Modelado). | `municipio`, `total_viajes_reales`, `potencial_teorico`, **`indice_cobertura`** |
| **GOLD** | `gold_flujos_provinciales` | KPI de Flujos Macroeconómicos Interprovinciales. | `provincia_origen`, `provincia_destino`, **`viajes_totales`**, `distancia_media_km` |

---

## Tech Stack

* **Language:** Python 3.x
* **Query Engine:** [DuckDB](https://duckdb.org/) (In-process SQL OLAP)
* **Data Access:** `pyspainmobility`
* **Data Manipulation:** Pandas
* **Visualization:** Seaborn / Matplotlib

## Getting Started

### Prerequisites
```bash
pip install pandas duckdb pyspainmobility seaborn matplotlib openpyxl
