use database snf_ev_data_demo;
use schema ev_population_data;
use role sysadmin;
use warehouse development;

/*
   Stages are synonymous with the idea of folders
   that can be either internal or external.
*/
create or replace stage azure
storage_integration = azure_integration
url = 'azure://<storage account name>.blob.core.windows.net/snfdata'  
directory = ( enable = true);

