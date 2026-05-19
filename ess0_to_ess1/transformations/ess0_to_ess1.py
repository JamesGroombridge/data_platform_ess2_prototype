import json
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, StructType, StructField, TimestampType, LongType, FloatType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from utilities.utility_datacontract import data_contract_list,get_dynamic_expressions
from utilities.utility_logging import event_hook, PipelineLogger


# set up spark session once for reuse
spark = SparkSession.builder.getOrCreate()

# intialise logger
logger = PipelineLogger(spark=spark, dataset_name="ess1")

logger.log("Starting ess0 to ees1 pipeline", log_type="system")

# validation rules
VALIDATION_RULE = """feature_id IS NOT NULL AND TRIM(feature_id) != ''"""

def run_pipeline(data_contract_elements):
    yaml_file = data_contract_elements['contract_path']
    properties = data_contract_elements['properties']
    column_projections = get_dynamic_expressions(properties)
    source = data_contract_elements['source']
    volume = data_contract_elements['volume']
    table_name = data_contract_elements['table_name']
    folder = data_contract_elements['folder']
    schema = data_contract_elements['schema']
    key = data_contract_elements['key']
    

    @dp.temporary_view(name=source)
    def temp_view():
        df_raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/Volumes/places/enterprise_steady_state/ess0_linz_json/_schema/{schema}")
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
        name=f"places.enterprise_steady_state.{volume}_quarantine",
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
        name=f"places.enterprise_steady_state.{volume}",
        comment="LINZ data table with full history tracking (SCD Type 2)"
    )

    dp.create_auto_cdc_flow(
        target=f"places.enterprise_steady_state.{volume}",
        source=f"{source}_validated",
        keys=[key],
        sequence_by="processing_timestamp",
        stored_as_scd_type=2)


# Event hook registered once at module level
@dp.on_event_hook
def slack_event_hook(event):
    event_hook(event, spark)


data_contract_list = data_contract_list()
for data_contract_element in data_contract_list:
    run_pipeline(data_contract_element)
