from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, StructType, StructField, TimestampType, LongType, FloatType
import yaml


def get_dynamic_projections(yaml_path):
    """
    Parses the YAML and returns a list of Spark Column expressions 
    mapping sourcePath to name with correct type casting.
    """
    # Type mapping for casting
    TYPE_MAPPING = {
        "string": StringType(),
        "double": DoubleType(),
        "float": FloatType(),
        "integer": IntegerType(),
        "long": LongType(),
        "boolean": BooleanType()
    }

    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    properties = config.get('schema', [{}])[0].get('properties', [])
    
    expressions = []
    for prop in properties:
        name = prop.get('name')
        source_path = prop.get('sourcePath')
        p_type = prop.get('physicalType', 'string').lower()
        spark_type = TYPE_MAPPING.get(p_type, StringType())

        if source_path:
            # Create a column expression from the source path and cast it
            expr = F.col(source_path).cast(spark_type).alias(name)
        else:
            # Handle null sourcePaths by creating a null literal of the correct type
            expr = F.lit(None).cast(spark_type).alias(name)
        
        expressions.append(expr)
        
    return expressions

# Generate the expressions from your YAML
yaml_file = "/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_data_contract/linz_data_contract_with_sourcepath.yml"
column_projections = get_dynamic_projections(yaml_file)

@dp.temporary_view()
def linz_property_unit_of_property():
    df_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/places/enterprise_steady_state/ess0_linz_json/_schema/linz_property_unit_of_property_changeset")
    .option("multiLine", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_property_unit_of_property_changeset/")
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
    name="places.enterprise_steady_state.linz_property_unit_of_property",
    comment="LINZ property unit of property data with full history tracking (SCD Type 2)"
)


dp.create_auto_cdc_flow(
    target="places.enterprise_steady_state.linz_property_unit_of_property",
    source="linz_property_unit_of_property",
    keys=["feature_id"],
    sequence_by="processing_timestamp",
    stored_as_scd_type=2)
