import requests
import json
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