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
# | 4 | `notebookutils.notebook.runMultiple` | Executes all project notebooks **in parallel** using the DAG — data prep, enriched, and the curated materialisations, each starting as soon as its own dependencies finish. Visualises the DAG via Graphviz. |
# | 5 | `03 - refresh sql endpoint` | Refreshes the SQL endpoints for the `Enr` (Enriched) and `Cur` (Curated) lakehouses after ETL completes. |
# | 6 | `04 - trigger semantic models refresh` | Refreshes Power BI semantic models. The list of workspaces/models is loaded from `GlobalParameters.get_semantic_models()`. |
# | 7 | Saturday cleanup | If today is **Saturday**, runs `Optimize lakehouses` to vacuum/optimise Delta tables and exits early. |
# 
# ---
# 
# ## Dependencies
# 
# - **`FM_Utility`** — utility functions and `GlobalParameters`
# - **`02 - Build dag`** — produces the `DAG` variable consumed by `runMultiple`, including the `curated_DAG` section
# - **`03 - refresh sql endpoint`** — refreshes the `Enr` and `Cur` lakehouse SQL endpoints
# - **`04 - trigger semantic models refresh`** — triggers Power BI dataset refreshes
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
# `Enr` for downstream SQL consumers, `Cur` so the curated tables built by the DAG are visible through the Cur SQL endpoint. Direct Lake on OneLake does not need either refresh, but the Finance Agent and the transitional `[pbi]` views do.

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

# CELL ********************

lakehouse_to_refresh = 'Cur'

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
