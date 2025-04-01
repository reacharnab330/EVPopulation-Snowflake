
from snowflake.snowpark import Session

def create_structured_table(session: Session, raw_table_name: str, structured_table_name: str) -> None:
    """
    Parses the raw JSON data into a structured table with flattened columns for vehicle data.
    
    Args:
        session: Snowpark session object
        raw_table_name: Name of the raw table containing JSON data
        structured_table_name: Name of the structured table to create
    """

    # Create the structured table based on the JSON schema with LATERAL FLATTEN for multiple rows
    create_structured_table_sql = f"""
    CREATE OR REPLACE TABLE {structured_table_name} AS
    SELECT
        file_name,
        f.value[8]::STRING AS vin_1_10,
        f.value[9]::STRING AS county,
        f.value[10]::STRING AS city,
        f.value[11]::STRING AS state,
        f.value[12]::STRING AS postal_code,
        f.value[13]::INTEGER AS model_year,
        f.value[14]::STRING AS make,
        f.value[15]::STRING AS model,
        f.value[16]::STRING AS electric_vehicle_type,
        f.value[17]::STRING AS cafv_eligibility,
        f.value[18]::INTEGER AS electric_range,
        f.value[19]::DOUBLE AS base_msrp,
        f.value[20]::INTEGER AS legislative_district,
        f.value[21]::STRING AS dol_vehicle_id,
        NULLIF(f.value[22], 'null')::STRING AS vehicle_location,  -- Handle 'null' string
        NULLIF(f.value[23], 'null')::STRING AS electric_utility,
        NULLIF(f.value[24], 'null')::STRING AS census_tract_2020,
        NULLIF(f.value[25], 'null')::INTEGER AS counties,
        NULLIF(f.value[26], 'null')::INTEGER AS congressional_districts,
        NULLIF(f.value[27], 'null')::INTEGER AS legislative_district_boundary
    FROM {raw_table_name},
    LATERAL FLATTEN(input => PARSE_JSON(DATA):data) f  -- Ensure 'data' column is used for flattening
    """
    session.sql(create_structured_table_sql).collect()

def create_approvals_table(session: Session, raw_table_name: str, approvals_table_name: str) -> None:
    """
    Extracts approvals and submitter information from the JSON and loads it into a separate table.
    
    Args:
        session: Snowpark session object
        raw_table_name: Name of the raw table containing JSON data
        approvals_table_name: Name of the table to store approvals and submitter data
    """
    # Create a table for approvals and submitter info
    create_approvals_table_sql = f"""
    CREATE OR REPLACE TABLE {approvals_table_name} AS
    SELECT
        file_name,
        TO_TIMESTAMP(a.value:reviewedAt::INTEGER) AS reviewed_at,  -- Convert epoch to TIMESTAMP
        a.value:reviewedAutomatically::BOOLEAN AS reviewed_automatically,
        a.value:state::STRING AS state,
        a.value:submissionId::INTEGER AS submission_id,
        a.value:submissionObject::STRING AS submission_object,
        a.value:submissionOutcome::STRING AS submission_outcome,
        TO_TIMESTAMP(a.value:submittedAt::INTEGER) AS submitted_at,  -- Convert epoch to TIMESTAMP
        a.value:workflowId::INTEGER AS workflow_id,
        a.value:submissionDetails.permissionType::STRING AS permission_type,
        a.value:submissionOutcomeApplication.failureCount::INTEGER AS failure_count,
        a.value:submissionOutcomeApplication.status::STRING AS outcome_status,
        a.value:submitter.id::STRING AS submitter_id,
        a.value:submitter.displayName::STRING AS submitter_display_name
    FROM {raw_table_name},
    LATERAL FLATTEN(input => GET_PATH(data, 'meta.view.approvals')) a
    """
    session.sql(create_approvals_table_sql).collect()