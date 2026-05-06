import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, StructType, StructField, TimestampType, LongType, FloatType
from utilities.utilities import get_dynamic_projections


# Generate the expressions from your YAML
yaml_file = "/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_data_contract/linz_data_contract_with_sourcepath.yml"
column_projections = get_dynamic_projections(yaml_file)

data_contract_list = [{'data_contract_path': '/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_data_contract/linz_data_contract_with_sourcepath.yml', 'source': 'linz_property_unit_of_property', 'volume': 'linz_property_unit_of_property', 'table_name': 'linz_property_unit_of_property', 'folder': 'linz_property_unit_of_property_changeset', 'schema':'linz_property_unit_of_property_changeset'}]

def run_pipeline(data_contract_elements):
    yaml_file = data_contract_elements['data_contract_path']
    column_projections = get_dynamic_projections(yaml_file)
    source = data_contract_elements['source']
    volume = data_contract_elements['volume']
    table_name = data_contract_elements['table_name']
    folder = data_contract_element['folder']
    schema = data_contract_elements['schema']

    @dp.temporary_view()
    def linz_property_unit_of_property():
        df_raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(f"cloudFiles.schemaLocation", "/Volumes/places/enterprise_steady_state/ess0_linz_json/_schema/{schema}")
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

    # Define the streaming table - schema inferred from query
    dp.create_streaming_table(
        name=f"places.enterprise_steady_state.{table_name}",
        comment="LINZ data table with full history tracking (SCD Type 2)"
    )

    dp.create_auto_cdc_flow(
        target=f"places.enterprise_steady_state.{volume}",
        source=source,
        keys=["feature_id"],
        sequence_by="processing_timestamp",
        stored_as_scd_type=2)

for data_contract_element in data_contract_list:
    run_pipeline(data_contract_element)
