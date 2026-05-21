"""
Pre-pipeline validation module
Validates source files before pipeline execution
"""
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def log_message(logger_instance, level, message):
    """
    Helper to log message via logger or print as fallback
    """
    if logger_instance:
        if level == "info":
            logger_instance.info(message)
        elif level == "error":
            logger_instance.error(message)
    else:
        # Fallback to print if no logger available
        print(f"[{level.upper()}] {message}")


def validate_source_files(data_contract_list, spark, logger_instance=None):
    """
    Validates source files for completeness, validity, and uniqueness
    
    Args:
        data_contract_list: List of data contract dictionaries with 'name' and 'volume' keys
        spark: SparkSession instance
        logger_instance: Optional logger instance for logging
    
    Returns:
        bool: True if all validations pass
        
    Raises:
        Exception: If any validation fails
    """
    dbutils = DBUtils(spark)
    
    log_message(logger_instance, "info", "Starting pre-pipeline file validation")
    
    current_time = datetime.now(timezone.utc)
    
    for schema in data_contract_list:
        name = schema['name']
        volume = schema['volume']
        
        # Check files exist
        try:
            files = dbutils.fs.ls(volume)
        except Exception as e:
            error_msg = f"Cannot access volume for {name}: {volume}"
            log_message(logger_instance, "error", error_msg)
            raise Exception(error_msg) from e
        
        if len(files) == 0:
            error_msg = f"No files found for {name} in {volume}"
            log_message(logger_instance, "error", error_msg)
            raise Exception(error_msg)
        
        log_message(logger_instance, "info", f"{name} - {len(files)} files found")
        
        # Validate each file
        for file in files:
            mod_time = datetime.fromtimestamp(file.modificationTime / 1000, tz=timezone.utc)
            age_seconds = (current_time - mod_time).total_seconds()
            age_days = int(age_seconds / 86400)
            size_kb = round(file.size / 1024, 2)
            
            # Check file size
            if size_kb < 2:
                error_msg = f"File {file.name} is less than 2KB for {name}"
                log_message(logger_instance, "error", error_msg)
                raise Exception(error_msg)
            
            # Check file age
            if age_days > 5:
                error_msg = f"File {file.name} is older than 5 days for {name}"
                log_message(logger_instance, "error", error_msg)
                raise Exception(error_msg)
        
        # Check for duplicate files
        file_names = [file.name for file in files]
        if len(file_names) != len(set(file_names)):
            error_msg = f"Duplicate files found for {name}"
            log_message(logger_instance, "error", error_msg)
            raise Exception(error_msg)
    
    log_message(logger_instance, "info", "Pre-pipeline file validation completed successfully")
    
    return True
