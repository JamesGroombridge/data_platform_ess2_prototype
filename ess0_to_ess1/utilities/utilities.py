from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, StructType, StructField, TimestampType, LongType, FloatType
import yaml


def get_dynamic_expressions(properties):
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

def data_contract_list():
    file_path = "/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_data_contract/linz_data_contract_prototype.yml"

    with open(file_path, "r") as f:
        contract = yaml.safe_load(f)
    
    schema_list = []

    # Iterate through each schema defined in the file
    for item in contract.get('schema', []):
            # Extract the requested fields at the same level
            schema_details = {
                'name': item.get('name'),
                'contract_path': item.get('contract_path'),
                'source': item.get('source'),
                'volume': item.get('volume'),
                'table_name': item.get('table_name'),
                'folder': item.get('folder'),
                'schema': item.get('schema'),
                'key': item.get('key'),
                'properties': item.get('properties', [])
            }
            schema_list.append(schema_details)

    return(schema_list)
    
        
