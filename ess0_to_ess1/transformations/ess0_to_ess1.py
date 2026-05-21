from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, StructType, StructField, TimestampType, LongType, FloatType
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from utilities.utility_datacontract import data_contract_list,get_dynamic_expressions
from utilities.utility_events import event_hook
from utilities.utility_validation import validate_source_files
from utilities.utility_logging import logger


# start logger
try:    
    logger_instance = logger("pipeline_validation")
    logger_instance.info(f"logger instance started")
except Exception as e:
    logger_instance.error(f"Logger instance failed: {e}")
    print(f"Logger initialization failed: {e}. Using print fallback.")
    logger_instance = None

# set up spark session once for reuse
spark = SparkSession.builder.getOrCreate()

# validation rules
VALIDATION_RULE = """feature_id IS NOT NULL AND TRIM(feature_id) != ''"""

# get schemas from data contract
try:
    data_schema_list = data_contract_list()
    logger_instance.info(f"Data schema from datacontract loaded successfully for {len(data_schema_list)} schemas")
except Exception as e:
    logger_instance.error(f"data schmea from datacontract faile: {e}. Using print fallback.")


# pre pipeline validate files before processing
try:
    validation_passed = validate_source_files(data_schema_list, spark, logger_instance)
    logger_instance.info(f"Pre-pipeline validation completed successfully for {len(data_schema_list)} schemas")
except Exception as validation_error:
    error_message = f"Pre-pipeline validation FAILED: {validation_error}"
    if logger_instance:
        logger_instance.error(error_message)
    # Re-raise to fail the pipeline immediately
    raise RuntimeError(f"Pipeline aborted due to validation failure: {validation_error}") from validation_error


# run pipeline
def run_pipeline(data_contract_elements):
    yaml_file = data_contract_elements['contract_path']
    properties = data_contract_elements['properties']
    source = data_contract_elements['source']
    volume = data_contract_elements['volume']
    table_name = data_contract_elements['table_name']
    folder = data_contract_elements['folder']
    schema = data_contract_elements['schema']
    key = data_contract_elements['key']
    
    # Get column projections
    column_projections = get_dynamic_expressions(properties)
    

    @dp.temporary_view(name=source)
    def temp_view():
        df_raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", "1")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"/Volumes/places/enterprise_steady_state/ess0_linz_json/{folder}/")
        )

        # Apply the transformations: Explode and then Project
        # Add processing timestamp for CDC sequencing
        df = (
            df_raw
            .withColumn("feature", F.explode("features"))
            .select(*column_projections)
            .withColumn("processing_timestamp", F.current_timestamp())
        )
        return df

    # Quarantine table for records that fail validation
    @dp.table(
        name=f"places.enterprise_steady_state.{table_name}_quarantine",
        comment="Records the data that failed the quality validation"
    )
    def quarantine_table():
        return (
            spark.readStream.table(source)
            .filter(f"NOT ({VALIDATION_RULE})")
            .withColumn("quarantine_timestamp", F.current_timestamp())
            .withColumn("validation_rule_failed", lit(VALIDATION_RULE))
        )

    # Validated view - drops invalid records before CDC
    @dp.temporary_view(name=f"{source}_validated")
    @dp.expect_or_drop("valid_id", VALIDATION_RULE)
    def validated_view():
        return spark.readStream.table(source)

    # Define the streaming table - schema inferred from query
    dp.create_streaming_table(
        name=f"places.enterprise_steady_state.{table_name}",
        comment="LINZ data table with full history tracking (SCD Type 2)"
    )

    # CDC flow
    dp.create_auto_cdc_flow(
        target=f"places.enterprise_steady_state.{table_name}",
        source=f"{source}_validated",
        keys=[key],
        sequence_by="processing_timestamp",
        stored_as_scd_type=2)


# Run pipeline for each schema - only executes if validation passed
for schema in data_schema_list:
    run_pipeline(schema)


# Event hook registered once at module level
@dp.on_event_hook
def slack_event_hook(event):
    event_hook(event, spark)


