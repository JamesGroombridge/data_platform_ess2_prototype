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
        return {"log_name": log_name, "log_volume": log_volume, "log_level": log_level, "metric_volume": metric_volume}
    except Exception as e:
        print(f"Error with data_contract read: {e}")
        raise


# 1. Custom Logger Subclass to support .metric()
class MetricsAndLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        self.metrics_volume = None
        self.dbutils = None
        self.notebook_name = name

    def metric(self, metric_type, metric_name, value, expected, unit="Count", dimensions=None):
        """
        Custom method to emit structured JSONL metric to a distinct volume.
        """
        if not self.metrics_volume or not self.dbutils:
            print("Metric volume or dbutils not initialized. Skipping metric write.")
            return

        try:
            # Re-use your existing job context extraction logic
            job_context = {}
            try:
                context_tags = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
                job_context = {
                    "job_id": context_tags.get("jobId", None),
                    "run_id": context_tags.get("multitaskParentRunId", None) or context_tags.get("runId", None),
                    "task_id": context_tags.get("taskKey", None)
                }
            except Exception:
                job_context = {"job_id": None, "run_id": None, "task_id": None}

            # Distinct Metrics Schema
            metric_record = {
                "timestamp": datetime.utcnow().isoformat(),
                "metric_type": metric_type,
                "metric_name": metric_name,
                "value": float(value),
                "expected": expected,
                "unit": unit,
                "dimensions": dimensions or {},
                "notebook": self.notebook_name,
                **job_context
            }

            # Save to distinct volume path
            file_name = f"metric_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
            full_path = f"{self.metrics_volume}/{file_name}"
            json_line = json.dumps(metric_record) + "\n"

            self.dbutils.fs.put(full_path, json_line, overwrite=True)

        except Exception as e:
            print(f"Metrics collection failure: {e}")

# Register the custom logger class with Python's logging library
logging.setLoggerClass(MetricsAndLogger)


# 2. Your Modified Factory Function
def logger(log_name):
    # Pull both configs from your data contract
    logger_config = config_operations_log()
    level = logger_config.get('log_level', 'INFO')
    log_volume = logger_config.get('log_volume')
    # Assuming your data contract can provide a metrics volume, otherwise default/derive it
    metrics_volume = logger_config.get('metric_volume', f"{log_volume}/_metric") 
    
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    log_level_upper = level.upper() if level else 'INFO'
    if log_level_upper not in valid_levels:
        print(f"Invalid log level '{level}', defaulting to INFO")
        log_level_upper = 'INFO'
    
    # This now safely returns our custom MetricsAndLogger instance
    logger_instance = logging.getLogger(log_name)
    logger_instance.setLevel(getattr(logging, log_level_upper))

    # Initialize Spark variables
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    # Attach volumes and dbutils context specifically for the .metric() method
    logger_instance.metrics_volume = metrics_volume
    logger_instance.dbutils = dbutils

    if logger_instance.handlers:
        logger_instance.handlers.clear()

    # The existing Log Handler (Kept completely separate for standard logs)
    class JsonFilePerEventHandler(logging.Handler):
        def _get_job_context(self):
            try:
                context_tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
                return {
                    "job_id": context_tags.get("jobId", None),
                    "run_id": context_tags.get("multitaskParentRunId", None) or context_tags.get("runId", None),
                    "task_id": context_tags.get("taskKey", None)
                }
            except Exception:
                return {"job_id": None, "run_id": None, "task_id": None}
        
        def emit(self, record):
            try:
                job_context = self._get_job_context()
                
                log_record = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                    "notebook": log_name,
                    **job_context
                }

                file_name = f"log_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
                full_path = f"{log_volume}/{file_name}"
                json_line = json.dumps(log_record) + "\n"

                dbutils.fs.put(full_path, json_line, overwrite=True)

            except Exception as e:
                print(f"Logging failure: {e}")

    handler = JsonFilePerEventHandler()
    logger_instance.addHandler(handler)

    return logger_instance
