# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#workspace:models[]
#reports = { "Dev Reports":["PowerBiDataModel", "Projects"]}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import sempy.fabric as fabric 

for workspace, models in reports.items():
    for model in models:
        # Refresh the dataset
        # Valid options for refresh_type: 'full', 'automatic', 'dataOnly', 'calculate', 'clearValues', 'defragment'. Default is 'automatic'.

        fabric.refresh_dataset(
            workspace=workspace,      # Defaults to "None" when not used -> current workspace
            dataset=model, 
            # objects=objects_to_refresh,   # No specific tabels or partitions specified -> refresh complete semantic model
            refresh_type = 'full'
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
