# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fabd3b09-a41c-4f27-83a3-289dabe677e5",
# META       "default_lakehouse_name": "DP",
# META       "default_lakehouse_workspace_id": "3a25df0d-986f-45a8-9140-8e9d88526e86",
# META       "known_lakehouses": [
# META         {
# META           "id": "fabd3b09-a41c-4f27-83a3-289dabe677e5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Master Orchestrator — Overview
# 
# This notebook is the **top-level orchestrator** for the data platform. It coordinates the full end-to-end data refresh pipeline by calling several downstream notebooks in sequence.
# 
# ---
# 
# ## What it does (step by step)
# 
# | Step | Notebook / Action | Description |
# |------|-------------------|-------------|
# | 1 | `FM_Utility` | Loads shared utility functions and global parameters used throughout the pipeline. |
# | 2 | ETL load mode | Determines load strategy: **full load** on Sundays (`isoweekday() == 7`), otherwise **incremental** with a 90-day window. *(Currently overridden to full load for development.)* |
# | 3 | `02 - Build dag` | Builds the DAG (Directed Acyclic Graph) that defines which project notebooks to run and in what order. |
# | 4 | `notebookutils.notebook.runMultiple` | Executes all project notebooks **in parallel** using the DAG. Visualises the DAG via Graphviz. |
# | 5 | `03 - refresh sql endpoint` | Refreshes the SQL endpoint for the `Enr` (Enriched) lakehouse after ETL completes. |
# | 6 | Curated Views init *(first run only)* | Triggers the `Run_Curated_Views` Data Pipeline in Microsoft Fabric to create Power BI views in the `Cur` SQL endpoint. Writes a marker file (`_master_orchestrator_initialized.txt`) so this step is skipped on all subsequent runs.<br><br>**For this to work all Curated Views notebooks must be attached to the CUR SQL Endpoint before running.** |
# | 7 | `04 - trigger semantic models refresh` | Refreshes Power BI semantic models. The list of workspaces/models is loaded from `GlobalParameters.get_semantic_models()`. |
# | 8 | Saturday cleanup | If today is **Saturday**, runs `Optimize lakehouses` to vacuum/optimise Delta tables and exits early. |
# 
# ---
# 
# ## Dependencies
# 
# - **`FM_Utility`** — utility functions and `GlobalParameters`
# - **`02 - Build dag`** — produces the `DAG` variable consumed by `runMultiple`
# - **`03 - refresh sql endpoint`** — refreshes the `Enr` lakehouse SQL endpoint
# - **`04 - trigger semantic models refresh`** — triggers Power BI dataset refreshes
# - **`Run_Curated_Views`** — Fabric Data Pipeline (triggered once via REST API)
# - **`Optimize lakehouses`** — weekly maintenance notebook


# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run the ETL in dataplatform

# CELL ********************

#Run the pipeline in incremental load for big tables, once a week do a full load just incase. 

if datetime.now().isoweekday() == 7:
    incremental_window =  None
    run_incremental = False
else:
    incremental_window =  90
    run_incremental = True


#Uncomment after development for full initial load.
incremental_window =  None
run_incremental = False

# Force a rebuild of the slow-changing dimensions (ENR_GLAccountHierarchy, ENR_AccountSchedule)
# even when their source fingerprint is unchanged. Default False -> self-skip when unchanged.
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orchestrate Projects

# CELL ********************

%run 02 - Build dag

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Pick notebooks to run (optional)
#
# The cell below renders a checkbox picker grouped by module. It's optional — the notebook runs everything by default.
#
# **Everyday full run:** leave this picker cell **frozen** (right-click the cell → *Freeze*). It's skipped, the picker isn't built, and the run cell falls back to a full run of the whole DAG. This is the production default.
#
# **Run only certain tables:** **unfreeze** this cell (right-click → *Unfreeze*) and run it. Tick the notebooks you want, then run the next cell — only the ticked notebooks run (in parallel via `runMultiple`), with each one's dependencies pruned to your selection. ⚠️ Make sure the upstream data for your selection already exists, since pruned dependencies won't be rebuilt.
#
# **Forgot to freeze?** No problem — an unfrozen picker with nothing ticked still does a full run. The only way to get a partial run is to actively tick boxes.

# CELL ********************

run_picker = Orchestration.build_run_picker(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Runs only the picked notebooks (in parallel, dependencies pruned to the selection), or the full DAG if nothing is picked. Both paths go through runMultiple.
# run_picker may be absent when the picker cell above is frozen/skipped — that means a full run.
Response = Orchestration.run(DAG, globals().get("run_picker"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Refresh SQL endpoint

# CELL ********************

lakehouse_to_refresh = 'Enr'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

%run 03 - refresh sql endpoint

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Initialize curated views (first run only)
# Creates the Power BI views in the Cur SQL endpoint by triggering the `Run_Curated_Views` pipeline. This only runs once — on subsequent runs it detects the marker file and skips. Delete `_master_orchestrator_initialized.txt` from the attached lakehouse Files folder to re-run.
# 
# **For this to work all Curated Views notebooks must be attached to the CUR SQL Endpoint before running.**

# CELL ********************

## Run curated views setup (first run only)
import os
import time
import sempy.fabric as fabric
from datetime import datetime

first_run_marker = "/lakehouse/default/Files/_master_orchestrator_initialized.txt"

if not os.path.exists(first_run_marker):
    print("First run detected — creating curated views...")

    client = fabric.FabricRestClient()
    workspace_id = fabric.resolve_workspace_id()
    pipeline_id = fabric.resolve_item_id("Run_Curated_Views", type="DataPipeline")

    # Trigger the pipeline
    response = client.post(f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline")
    
    if response.status_code == 202:
        job_location = response.headers.get("Location")
        print("Pipeline triggered — waiting for completion...")

        while True:
            status_response = client.get(job_location)
            status = status_response.json().get("status")
            print(f"  Status: {status}")

            if status in ("Completed", "Failed", "Cancelled"):
                break
            time.sleep(15)

        if status == "Completed":
            with open(first_run_marker, "w") as f:
                f.write(str(datetime.now()))
            print("Curated views created successfully.")
        else:
            raise Exception(f"Pipeline finished with status: {status}")
    else:
        raise Exception(f"Failed to trigger pipeline. Status code: {response.status_code}")

else:
    print("Curated views already initialized — skipping.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Refresh semantic model

# CELL ********************

#format -> {workspace:models[]}
# reports = { 
#     "Dev Reports":["PowerBiDataModel", "Projects"]
#     #"Test Reports":["PowerBiDataModel", "Projects"]
# }

reports = GlobalParameters.get_semantic_models()
print(reports)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run 04 - trigger semantic models refresh

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Vacuum the tables if it's saturday

# CELL ********************

# incase it's cleaning day clean up the enviroment

if datetime.now().isoweekday() == 7:
    notebookutils.notebook.run("Optimize lakehouses")
    notebookutils.notebook.exit("Cleaned up the lakehouses")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
