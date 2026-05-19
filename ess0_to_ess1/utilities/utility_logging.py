import logging
import json
import os
import requests
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pyspark.dbutils import DBUtils


def utc_now():
    return datetime.utcnow().isoformat() + "Z"

def get_dlt_context(spark):
    """Extract Databricks Declarative Pipeline context safely."""
    try:
        return {
            "run_id": spark.conf.get("spark.databricks.pipeline.runId", None),
            "pipeline_id": spark.conf.get("spark.databricks.pipeline.id", None),
            "pipeline_name": spark.conf.get("spark.databricks.pipeline.name", None),
        }
    except Exception:
        return {}

class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": utc_now(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)

class PipelineLogger:
    # catalog would be places
    DEFAULT_PATH = "/Volumes/places/enterprise_steady_state/operations/logs/"
    def __init__(
        self,
        spark=None,
        name="data_pipeline",
        path=None,
        dataset_name=None,
        task_id=None,
    ):
        self.name = name
        self.path = path or self.DEFAULT_PATH
        self.spark = spark
        self.dataset_name = dataset_name
        self.task_id = task_id or dataset_name

        self.logger = logging.getLogger(name)

        # Only configure handlers if not already configured
        if self.logger.handlers:
            return

        self.logger.setLevel(logging.INFO)
        formatter = JsonFormatter()

        # Console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

        # File (hourly rotation)
        if self.path:
            try:
                os.makedirs(self.path, exist_ok=True)
                # Base filename without timestamp - TimedRotatingFileHandler adds suffix on rotation
                file_path = os.path.join(self.path, f"{name}.log")
                file_handler = TimedRotatingFileHandler(
                    filename=file_path,
                    when="H",
                    interval=1,
                    backupCount=48,
                    utc=True,
                )
                file_handler.suffix = "%Y-%m-%d_%H"

                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception as e:
                print(f"WARNING: file logging disabled: {e}")

    def _get_current_context(self):
        """Fetch current pipeline context dynamically at log time."""
        dlt_context = get_dlt_context(self.spark) if self.spark else {}
        return {
            "run_id": dlt_context.get("run_id"),
            "pipeline_id": dlt_context.get("pipeline_id"),
            "pipeline_name": dlt_context.get("pipeline_name"),
            "dataset_name": self.dataset_name,
            "task_id": self.task_id,
        }

    def log(self, message, level="info", log_type="informational", **kwargs):
        extra_fields = {
            "log_type": log_type,
            **self._get_current_context(),
            **kwargs,
        }

        log_method = getattr(self.logger, level.lower(), self.logger.info)

        log_method(
            message,
            extra={"extra_fields": extra_fields},
        )

    def log_metric(self, name, value, step=None, **kwargs):
        self.log(
            message=f"Metric: {name}",
            level="info",
            log_type="metric",
            metric_name=name,
            metric_value=value,
            pipeline_step=step or self.task_id,
            **kwargs,
        )

def event_hook(event,spark):
    try:
        if (event['event_type'] == 'update_progress'):
            # Get dbutils from spark context (required in pipeline files)
            dbutils = DBUtils(spark)
            # get secret from scope slack
            slack_webhook_url = dbutils.secrets.get(scope="slack", key="webhook_url")
            # Convert event object to formatted string
            event_type = event['event_type']
            state = ''
            if event_type == 'update_progress':
                state = event['details']['update_progress']['state']
            text = f"event type: {event_type} state: {state}"
            payload = {"text": text}
            headers = {'Content-Type': 'application/json'}
            # send to slack
            response = requests.post(slack_webhook_url, headers=headers, json=payload, timeout=5)
    except Exception as e:
        print(f"Error sending Slack message: {e}")