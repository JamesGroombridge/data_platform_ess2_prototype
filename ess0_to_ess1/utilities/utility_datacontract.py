import yaml
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, StructType, StructField, TimestampType, LongType, FloatType



def data_contract_list(logger_instance):
    try:
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
        logger_instance.info(f"schema list success")
        return(schema_list)
    except Exception as e:
        logger_instance.error(f"Error in data_contract_list: {e}")

def get_dynamic_expressions(properties, logger_instance):
    try:
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
        logger_instance.info(f"expressions success for schema")             
        return expressions
    except Exception as e:
        logger_instance.error(f"Error in get_dynamic_expressions: {e}")


    

        
