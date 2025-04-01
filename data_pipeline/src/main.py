from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, parse_json
import json
from datetime import datetime
import toml
import os
import time

from ingestion import create_raw_table, load_json_from_stage
from transformation import create_structured_table, create_approvals_table
from validation import validate_data


# Main execution
def main():
    
    # Start timing
    start_time = time.time()

    print("Current working directory:", os.getcwd())

    # Load connection parameters from TOML file
    with open('src/connections.toml', 'r') as f:
        connection_parameters = toml.load(f)['default']

    session = Session.builder.configs(connection_parameters).create()
    

    # Parameters 
    raw_table_name = "RAW_EV_DATA"
    structured_table_name = "TRANSFORMED_EV_DATA"
    approvals_table_name = "TRANSFORMED_APPROVALS_DATA"
    validation_Result_table_name = "VALIDATION_RESULTS"
    validated__data_table_name = "VALIDATED_EV_DATA" 

    

    stage_name = "AZURE"
    file_path = "ElectricVehiclePopulationData.json"  # Adjust path as needed
    current_year = datetime.now().year

    #Ingestion of data from stage -  loading the raw table 
    create_raw_table(session, raw_table_name)
    load_json_from_stage(session, stage_name, file_path, raw_table_name)
    
    # Transformation - flattening the json and loading the ev_population and ev_approval tables
    create_structured_table(session, raw_table_name, structured_table_name)  # Vehicle data
    create_approvals_table(session, raw_table_name, approvals_table_name)    # Approvals data
    validate_data(session, structured_table_name, validation_Result_table_name, validated__data_table_name, current_year)
    
    # Calculate and print execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Pipeline execution completed in {execution_time:.2f} seconds")
    
    # Close the session
    session.close()

if __name__ == "__main__":
    main()