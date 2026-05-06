# Databricks notebook source
# MAGIC %md
# MAGIC Edit base file to make it a changeset

# COMMAND ----------

# DBTITLE 1,Cell 2
import json
from datetime import datetime

def base_to_changeset(change_type):
    try:
        dt_string = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_name = f"property_unit_of_property_changeset_5_{dt_string}.json"


        volume_path = "/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_property_unit_of_property_base/property_unit_of_property_base_5.json"
        output_path = F"/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_property_unit_of_property_changeset/{output_name}"

        # 1. Read from Volume using standard Python open
        with open(volume_path, "r") as f:
            data = json.load(f)

        # 2. Use a for loop to update the nested list
        # Assuming 'items' is your list of dictionaries
        for feature in data["features"]:
            feature["properties"]["__change__"] = change_type

        # 3. Save it back
        with open(output_path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error: {e}")

base_to_changeset("INSERT")

# COMMAND ----------

# MAGIC %md
# MAGIC Modify Changeset

# COMMAND ----------

import json
from datetime import datetime

def changeset_to_changeset(change_type, input_name):
    try:
        dt_string = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_name = f"property_unit_of_property_changeset_5_{dt_string}.json"


        volume_path = F"/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_property_unit_of_property_changeset/{input_name}"
        output_path = F"/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_property_unit_of_property_changeset/{output_name}"

        # 1. Read from Volume using standard Python open
        with open(volume_path, "r") as f:
            data = json.load(f)

        # 2. Use a for loop to update the nested list
        # Assuming 'items' is your list of dictionaries
        for feature in data["features"]:
            feature["properties"]["__change__"] = change_type

        # 3. Save it back
        with open(output_path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error: {e}")

changeset_to_changeset("UPDATE", "property_unit_of_property_changeset_5_2026-05-06_00-50-20.json")
