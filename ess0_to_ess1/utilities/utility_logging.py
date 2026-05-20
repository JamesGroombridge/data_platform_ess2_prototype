import logging
import json
import os
import sys
import requests
import yaml
from datetime import datetime
from pyspark.dbutils import DBUtils

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


def config_operations_log():
    try:
        # file path to data_contratc holding server properties
        file_path = "/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_data_contract/linz_data_contract_prototype.yml"

        with open(file_path, "r") as f:
            contract = yaml.safe_load(f)

        # parse out log_name and log_volume
        servers = contract.get("servers", {})
        development_server = servers.get("development", {})
        log_name = development_server.get("log_name", "")
        log_volume = development_server.get("log_volume", "")
        log_level = development_server.get("log_level", "")
        return {"log_name": log_name, "log_volume": log_volume, "log_level": log_level}
    except Exception as e:
        print(f"Error with data_contract read: {e}")
        # Return default values instead of None
        return {"log_name": "pipeline_operations.json", 
                "log_volume": "/Volumes/places/enterprise_steady_state/ess0_linz_json/logs", 
                "log_level": "INFO"}
 
