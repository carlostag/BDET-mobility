# BDET-mobility
Project for Big Data Engineering Technologies made by Julia García and Carlos Torregrosa.

This document will act as kind of a draft of the different decissions made simulating the sprints used in agile methodologies.

1. Data adquisition.
We looked first for the data in the MITMA web, available in this link: https://www.transportes.gob.es/ministerio/proyectos-singulares/estudios-de-movilidad-con-big-data/opendata-movilidad
We first noticed that was nearly impossible to manually download every monthly dataset in terms of physical space and downloading time as each one had a weight of 5GB, so we observed the ministry offered a python package accesible called pyspainmobility. From their package web (https://pyspainmobility.github.io/pySpainMobility/reference/mobility.html) we get this info:

Parameters:

        version (int) – The version of the data to download. Default is 2. Version must be 1 or 2. Version 1 contains the data from 2020 to 2021. Version 2 contains the data from 2022 onwards.

        zones (str) – The zones to download the data for. Default is municipalities. Zones must be one of the following: districts, dist, distr, distritos, municipalities, muni, municipal, municipios, lua, large_urban_areas, gau, gaus, grandes_areas_urbanas

        start_date (str) – The start date of the data to download. Date must be in the format YYYY-MM-DD. A start date is required

        end_date (str) – The end date of the data to download. Default is None. Date must be in the format YYYY-MM-DD. if not specified, the end date will be the same as the start date.

        output_directory (str) – The directory to save the raw data and the processed parquet. Default is None. If not specified, the data will be saved in a folder named ‘data’ in user’s home directory.

        use_dask (bool) – Whether to use Dask for processing large datasets. Default is False. Requires dask to be installed.

2. Data exploration
Now, checking the GitHub repository more in depth, they have an examples folder where they share some use cases for this package. We'll use the one called "examples/01-madrid.ipynb" but applied to Valencia.
Example: Mean distance of the mobility of the population is 70.09km.

References:
Beneduce, C., Gullón Muñoz-Repiso, T., Lepri, B., & Luca, M. (2025). pySpainMobility: a Python Package to Access and Manage Spanish Open Mobility Data
