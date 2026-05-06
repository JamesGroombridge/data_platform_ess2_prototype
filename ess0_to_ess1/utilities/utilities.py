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