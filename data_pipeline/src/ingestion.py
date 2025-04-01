from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, parse_json
import json

def create_raw_table(session: Session, table_name: str) -> None:
    """
    Creates a table in Snowflake to store raw JSON data if it doesn't exist.
    
    Args:
        session: Snowpark session object
        table_name: Name of the raw table to create
    """
    drop_table_sql = f"""
    DROP TABLE IF EXISTS {table_name} 
    """

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        file_name VARCHAR,
        data VARIANT
    )
    """
    session.sql(drop_table_sql).collect()
    session.sql(create_table_sql).collect()


def load_json_from_stage(session: Session, stage_name: str, file_path: str, table_name: str) -> None:
    """
    Loads JSON data from an Azure external stage into a Snowflake table.
    
    Args:
        session: Snowpark session object
        stage_name: Name of the Azure external stage (e.g., 'my_azure_stage')
        file_path: Path to the JSON file in the stage (e.g., 'ev_data/ElectricVehiclePopulationData2.json')
        table_name: Name of the raw table to load data into
        match_by_column_name: Boolean flag to match by column name (case insensitive)
    """
    # Define the file format for JSON
    session.sql("""
    CREATE OR REPLACE FILE FORMAT json_format 
    TYPE = 'JSON' 
    --MULTILINE = TRUE
    """).collect()
    

    
    # Copy data from the stage into the table
    
    copy_sql = f"""
    COPY INTO {table_name}
    FROM (
        select 
        metadata$filename,
        $1
        from
        @{stage_name}
        (FILE_FORMAT => 'json_format') )
    """

    #print(copy_sql)
    session.sql(copy_sql).collect()


