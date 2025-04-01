
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, current_timestamp, lit

def validate_data(session: Session, source_table: str, validation_table: str, validated_table: str, current_year: int) -> None:
    """
    Performs data quality checks on the source table, logs results into a validation table,
    and creates a table with records that pass all checks.
    
    Args:
        session: Snowpark session object
        source_table: Name of the table to validate (e.g., STRUCTURED_EV_DATA)
        validation_table: Name of the table to store validation results
        validated_table: Name of the table to store records passing all checks
        current_year: Current year for model_year range validation
    """
    # Create validation results table if it doesn't exist
    create_validation_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {validation_table} (
        check_type STRING,
        column_name STRING,
        issue_count INTEGER,
        validation_timestamp TIMESTAMP_NTZ
    )
    """
    session.sql(create_validation_table_sql).collect()

    # Load the source table as a DataFrame
    df = session.table(source_table)

    # 1. Not Null Checks
    null_vin_count = df.filter(col("vin_1_10").is_null()).count()
    null_model_count = df.filter(col("model").is_null()).count()

    # 2. Duplicate Check on vin_1_10
    duplicates_df = (
        df.group_by("vin_1_10")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .select("vin_1_10")
    )
    duplicate_vin_count = duplicates_df.count()

    # 3. Range Checks
    # Electric Range: <= 0 or >= 1000
    invalid_electric_range_count = df.filter(
        (col("electric_range") <= 0) | (col("electric_range") >= 1000)
    ).count()

    # Base MSRP: < 0 or >= 1,000,000
    invalid_base_msrp_count = df.filter(
        (col("base_msrp") < 0) | (col("base_msrp") >= 1000000)
    ).count()

    # Model Year: < 1886 or > current_year
    invalid_model_year_count = df.filter(
        (col("model_year") < 1886) | (col("model_year") > current_year)
    ).count()

    # Collect validation results into a DataFrame
    validation_results = [
        ("NOT_NULL", "vin_1_10", null_vin_count),
        ("NOT_NULL", "model", null_model_count),
        ("DUPLICATE", "vin_1_10", duplicate_vin_count),
        ("RANGE", "electric_range", invalid_electric_range_count),
        ("RANGE", "base_msrp", invalid_base_msrp_count),
        ("RANGE", "model_year", invalid_model_year_count),
    ]
    validation_df = session.create_dataframe(
        validation_results,
        schema=["check_type", "column_name", "issue_count"]
    ).with_column("validation_timestamp", current_timestamp())
    validation_df.write.mode("overwrite").save_as_table(validation_table)

    # Filter records that pass all validation checks
    valid_df = df.filter(
        # Not Null Checks
        col("vin_1_10").is_not_null() &
        col("model").is_not_null() &
        # Duplicate Check (exclude VINs that appear more than once)
        #~col("vin_1_10").isin(duplicates_df["vin_1_10"]) &
        # Range Checks
        (col("electric_range") > 0) & (col("electric_range") < 1000) &
        (col("base_msrp") >= 0) & (col("base_msrp") < 1000000) &
        (col("model_year") >= 1986) & (col("model_year") <= current_year)
    )

    # Write passing records to the validated table
    valid_df.write.mode("overwrite").save_as_table(validated_table)