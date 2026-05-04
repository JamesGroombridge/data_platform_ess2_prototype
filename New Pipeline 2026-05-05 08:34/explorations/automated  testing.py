# Databricks notebook source
# MAGIC %md
# MAGIC Edit base file to make it a changeset

# COMMAND ----------

# DBTITLE 1,Cell 2
import json
from datetime import datetime

dt_string = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_name = f"property_unit_of_property_changeset_5_{dt_string}.json"


volume_path = "/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_property_unit_of_property_base/property_unit_of_property_base_5.json"
output_path = F"/Volumes/places/enterprise_steady_state/ess0_linz_json/linz_property_unit_of_property_changeset/{output_name}"

print(output_path)

# 1. Read from Volume using standard Python open
with open(volume_path, "r") as f:
    data = json.load(f)

# 2. Use a for loop to update the nested list
# Assuming 'items' is your list of dictionaries
for item in data["features"]:
    item["properties"]["__change__"] = "UPDATE"

# 3. Save it back
with open(output_path, "w") as f:
    json.dump(data, f, indent=4)
