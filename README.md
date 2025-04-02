# EVPopulation-Snowflake

This is a sample implementation of data processing pipeline for ingestion and processing of a JSON format sample dataset (ElectricVehiclePopulationData.json) using in Snowflake using Snowflake Snowpark APIs. n this data pipeline, it is assumed that the source data file is in an Azure Blob storage.

Architecture

<img src=https://github.com/reacharnab330/EVPopulation-Snowflake/blob/main/solution_arch_snf.PNG>

# Project Overview

The project has only one component :
1. Ingestion , processing along with data quality check implementation of sample JSON format data file ElectricVehiclePopulationData.json from Azure blob storage

# Data Model

Folloiwng the multilayer datalake architecture the following datasets are created in different layers :

1. Raw data layer         : raw_ev_data
2. Transformed data layer : transformed_ev_data, transformed_approvals_data
3. Validated data layer   : validated_ev_data, validation_results

# How to use this project

1. Clone this repo
2. In vscode navigate to the location where the repo was cloned data_pipeline/src
3. Create a Virtual Environment in vscode 
4. Install the required dependencies :
     1. pip install snowflake-snowpark-python
     2. pip install "snowflake-snowpark-python[pandas]"
6. run main.py
