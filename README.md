# EVPopulation-Snowflake

This is a sample implementation of data processing pipeline for ingestion and processing of a JSON format sample dataset (ElectricVehiclePopulationData.json) using in Snowflake using Snowflake Snowpark APIs. n this data pipeline, it is assumed that the source data file is in an Azure Blob storage.

Architecture

<img src=https://github.com/reacharnab330/EVPopulation-AzureDatabricks/blob/main/solution_arch_adb.PNG>

# Project Overview

The project has only one component :
1. Ingestion , processing along with data quality check implementation of sample JSON format data file ElectricVehiclePopulationData.json from Azure blob storage

# Data Model

Folloiwng the multilayer datalake architecture the following datasets are created in different layers :

1. Raw data layer         : raw_ev_data
2. Transformed data layer : transformed_ev_data, transformed_approvals_data
3. Validated data layer   : validated_ev_data, validation_results

# How to use this project

1. Clone the repo
