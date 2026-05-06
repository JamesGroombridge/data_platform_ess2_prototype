from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import yaml 
from pyspark.sql.types import *


def odcs_to_pyspark_schema(yml_path):
    with open(yml_path, 'r') as file:
        contract = yaml.safe_load(file)
    
    # Mapping ODCS logical types to PySpark types
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "number": DoubleType(), # Or DecimalType(10,2) based on physicalType
        "date": DateType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType()
    }
    
    fields = []
    
    # ODCS v3 structure: schema is a list, we take the first table ('orders')
    properties = contract['schema'][0]['properties']
    
    for prop in properties:
        name = prop['name']
        logical_type = prop.get('logicalType', 'string')
        # Check if the field is marked as required (nullable = not required)
        nullable = not prop.get('required', False)     
        spark_type = type_mapping.get(logical_type.lower(), StringType())       
        fields.append(StructField(name, spark_type, nullable))
   
    return StructType(fields)