import logging
import json
import os
import sys
import requests
import yaml
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def config_operations_log():
    try:
        # file path to data_contract holding server properties
        file_path = "/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_data_contract/linz_data_contract_prototype.yml"

        with open(file_path, "r") as f:
            contract = yaml.safe_load(f)

        # parse out log_name and log_volume
        servers = contract.get("servers", {})
        development_server = servers.get("development", {})
        log_name = development_server.get("log_name", "pipeline_operations")
        log_volume = development_server.get("log_volume", "")
        metric_volume = development_server.get("metric_volume", "")
        log_level = development_server.get("log_level", "INFO")
        return {"log_name": log_name, "log_volume": log_volume, "log_level": log_level}
    except Exception as e:
        print(f"Error with data_contract read: {e}")
        raise


def logger(log_name):
    # get config from data_contract
    logger_config = config_operations_log()
    level = logger_config['log_level']
    log_volume = logger_config['log_volume']
    
    # Validate and set log level
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    log_level_upper = level.upper() if level else 'INFO'
    if log_level_upper not in valid_levels:
        print(f"Invalid log level '{level}', defaulting to INFO")
        log_level_upper = 'INFO'
    
    logger_instance = logging.getLogger(log_name)
    logger_instance.setLevel(getattr(logging, log_level_upper))

    # Clear existing handlers to ensure fresh configuration
    if logger_instance.handlers:
        logger_instance.handlers.clear()

    # Initialize dbutils
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    class JsonFilePerEventHandler(logging.Handler):
        def _get_job_context(self):
            """Get Databricks job context if available."""
            try:
                context_tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
                return {
                    "job_id": context_tags.get("jobId", None),
                    "run_id": context_tags.get("multitaskParentRunId", None) or context_tags.get("runId", None),
                    "task_id": context_tags.get("taskKey", None)
                }
            except Exception:
                # Context tags not available in interactive notebook sessions
                return {"job_id": None, "run_id": None, "task_id": None}
        
        def emit(self, record):
            try:
                # Get Databricks job context
                job_context = self._get_job_context()
                
                log_record = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                    "notebook": log_name,
                    **job_context
                }

                # unique file per log event
                file_name = f"log_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
                full_path = f"{log_volume}/{file_name}"

                # JSONL = single JSON object per line
                json_line = json.dumps(log_record) + "\n"

                dbutils.fs.put(full_path, json_line, overwrite=True)

            except Exception as e:
                print(f"Logging failure: {e}")

    handler = JsonFilePerEventHandler()
    logger_instance.addHandler(handler)

    return logger_instance
