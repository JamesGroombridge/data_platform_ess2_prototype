from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


# check all new files are avialble for pipeline to run - completeness
# check files are appropriately sized - validity
# check there is not duplicated files - uniqueness
# log events to volume
# log metrics to volume
# trigger pipeline to run if criteria met


def validate_source_files(data_contract_list, spark, logger_instance):
    dbutils = DBUtils(spark)
    
    current_time = datetime.now(timezone.utc)
    
    for schema in data_contract_list:
        name = schema['name']
        volume = schema['volume']
        
        # Check files exist
        try:
            files = dbutils.fs.ls(volume)
            logger_instance.info(f"Found {len(files)} files for {name} in {volume}")            
        except Exception as e:
            logger_instance.error(f"Cannot access volume for {name}: {volume}")
            raise Exception(f"Cannot access volume for {name}: {volume}") from e
        
        if len(files) == 0:
            logger_instance.error(f"No files found for {name} in {volume}")
            raise Exception(f"No files found for {name} in {volume}")
        
        # Validate each file
        for file in files:
            mod_time = datetime.fromtimestamp(file.modificationTime / 1000, tz=timezone.utc)
            age_seconds = (current_time - mod_time).total_seconds()
            age_days = int(age_seconds / 86400)
            size_kb = round(file.size / 1024, 2)
            
            # Check file size
            if size_kb < 2:
                logger_instance.error(f"File {file.name} is less than 2KB for {name}")
                raise Exception(f"File {file.name} is less than 2KB for {name}")
            
            # Check file age
            if age_days > 5:
                logger_instance.error(f"File {file.name} is older than 5 days for {name}")
                raise Exception(f"File {file.name} is older than 5 days for {name}")
        
        # Check for duplicate files
        file_names = [file.name for file in files]
        if len(file_names) != len(set(file_names)):
            logger_instance.error(f"Duplicate files found for {name}")
            raise Exception(f"Duplicate files found for {name}")
    
    return True
