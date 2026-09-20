# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ### DP

# CELL ********************

DAG = {
    "activities": [
          {
            "name": "dp_items",
            "path": "dp_items",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_genProductPostingGroups",
            "path": "dp_genProductPostingGroups",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_genBusinessPostingGroups",
            "path": "dp_genBusinessPostingGroups",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_CurrencyExchangeRate",
            "path": "dp_CurrencyExchangeRate",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_DimensionValues",
            "path": "dp_DimensionValues",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_DimensionSetEntry",
            "path": "dp_DimensionSetEntry",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_company",
            "path": "dp_company",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": []
        },
    ],
    "timeoutInSeconds": 3600,
    "concurrency": 20
}

# Tag the base data-prep activities so the orchestrator's notebook picker can group them.
for a in DAG["activities"]:
    a["module"] = "Data Prep"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Finance DP

# CELL ********************

finance_dp_DAG = [
        {
            "name": "dp_glAccounts",
            "path": "dp_glAccounts",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_glEntry",
            "path": "dp_glEntry",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True,
                "run_incremental" : run_incremental,
                "incremental_window": incremental_window
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "dp_budget",
            "path": "dp_budget",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
]

for item in finance_dp_DAG:
    item["module"] = "Data Prep"
    DAG["activities"].append(item)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Enriched Cross-Module

# CELL ********************

shared_enriched_DAG = [
        {
            "name": "ENR_Calendar",
            "path": "ENR_Calendar",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True,
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": []
        },
        {
            "name": "ENR_Contact",
            "path": "ENR_Contact",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "ENR_CurrencyExchangeRates",
            "path": "ENR_CurrencyExchangeRates",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_CurrencyExchangeRate"
            ]
        },
        {
            "name": "ENR_CreateDimensions",
            "path": "ENR_CreateDimensions",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_DimensionValues"
            ]
        },
        {
            "name": "ENR_Company",
            "path": "ENR_Company",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": ["dp_company"]
        },
        {
            "name": "ENR_Item",
            "path": "ENR_Item",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": ["dp_items"]
        },
        {
            "name": "ENR_DimensionSetEntry",
            "path": "ENR_DimensionSetEntry",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_DimensionSetEntry",
            ]
        },
        {
            "name": "ENR_GenProductPostingGroups",
            "path": "ENR_GenProductPostingGroups",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_genProductPostingGroups"
            ]
        },
        {
            "name": "ENR_GenBusinessPostingGroups",
            "path": "ENR_GenBusinessPostingGroups",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_genBusinessPostingGroups"
            ]
        },
        {
            "name": "ENR_Vendor",
            "path": "ENR_Vendor",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "ENR_SalespersonPurchaser",
            "path": "ENR_SalespersonPurchaser",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
        {
            "name": "ENR_Customer",
            "path": "ENR_Customer",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company"
            ]
        },
]

for item in shared_enriched_DAG:
    item["module"] = "Shared"
    DAG["activities"].append(item)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Enriched Finance

# CELL ********************

finance_DAG = [
        {
            "name": "ENR_GLAccount",
            "path": "ENR_GLAccount",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_glAccounts",
                "dp_company"
            ]
        },
        {
            "name": "ENR_GLEntries",
            "path": "ENR_GLEntries",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True,
                "run_incremental" : run_incremental,
                "incremental_window": incremental_window
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_glEntry",
                "ENR_DimensionSetEntry",
                "ENR_GLAccount",
                "dp_company",
                "ENR_GenBusinessPostingGroups",
                "ENR_GenProductPostingGroups",
                "ENR_CurrencyExchangeRates"
            ]
        },
        {
            "name": "ENR_Budget",
            "path": "ENR_Budget",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_budget",
                "ENR_DimensionSetEntry",
                "ENR_GLAccount",
                "dp_company",
            ]
        },
        {
            "name": "ENR_AccountSchedule",
            "path": "ENR_AccountSchedule",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True,
                "force_refresh": force_refresh
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": [
                "dp_company",
                "dp_glAccounts"
            ]
        },
        {
            "name": "ENR_GLAccountHierarchy",
            "path": "ENR_GLAccountHierarchy",
            "timeoutPerCellInSeconds": 6000,
            "args": {
                "useRootDefaultLakehouse": True,
                "force_refresh": force_refresh
            },
            "retry": 1,
            "retryIntervalSeconds": 60,
            "dependencies": []
        },
]

for item in finance_DAG:
    item["module"] = "Finance"
    DAG["activities"].append(item)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Curated
