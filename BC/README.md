# BC Standard Accelerator — Fabric

The Fabric side of the BC Standard Accelerator: a ready-made analytics platform for Microsoft Business Central clients built on Microsoft Fabric.

> The companion repo [PowerBI_std](https://dev.azure.com/fellowminddk/BC%20BI%20standardmodel/_git/PowerBI_std) holds the Power BI semantic model and reports. Both repos are deployed together per client.

## Architecture

```
Business Central
      ↓  BC2ADLS
01 Raw Lakehouse          ← raw BC export, one shortcut per BC company
      ↓  Data Preparation notebooks
02 DP Lakehouse           ← standardised, lightly typed
      ↓  Enriched notebooks (per module)
03 Enriched Lakehouse     ← business-ready Delta tables
      ↓  Curated T-SQL views
04 Curated Lakehouse      ← SQL endpoint consumed by Power BI
      ↓
Power BI Semantic Model (PowerBI_std)
```

## Modules

Each client gets only the modules they have purchased. Unpurchased modules are pruned at deploy time.

| Module | Folder |
|---|---|
| Finance | `03 Enriched/Notebooks/Finance` |
| Sales | `03 Enriched/Notebooks/Sales` |
| Purchase | `03 Enriched/Notebooks/Purchase` |
| Accounts Receivable | `03 Enriched/Notebooks/AR` |
| Accounts Payable | `03 Enriched/Notebooks/AP` |
| Inventory | `03 Enriched/Notebooks/Inventory` |
| Project | `03 Enriched/Notebooks/Project` |

Cross-module logic (dimension sets, currency, shared lookups) lives in `Cross-Module` subfolders at the DP and Enriched layers.

## Repo structure

```
Fabric_std/
├── 01 Raw/
│   └── Raw.Lakehouse/              ← Fabric Lakehouse item (shortcut to BC2ADLS storage)
├── 02 Data preparation/
│   ├── DP.Lakehouse/
│   └── Notebooks/
│       ├── Cross-Module/
│       └── Finance/
├── 03 Enriched/
│   ├── Enr.Lakehouse/
│   └── Notebooks/
│       ├── Cross-Module/
│       ├── Finance/ Sales/ Purchase/ AR/ AP/ Inventory/ Project/
│       └── _Template/              ← template for new module notebooks
├── 04 Curated/
│   ├── Cur.Lakehouse/
│   ├── Curated Views/              ← T-SQL view notebooks + Run_Curated_Views pipeline
│   └── Notebooks/
│       └── CUR_Calendar.Notebook
├── Orchestration/
│   ├── 01 - Master orchestrator.Notebook   ← entry point; runs full end-to-end
│   ├── 02 - Build dag.Notebook             ← builds module execution graph
│   ├── 03 - refresh sql endpoint.Notebook
│   └── 04 - trigger semantic models refresh.Notebook
├── Utility/
│   ├── Workspace_Provisioner.Notebook      ← creates lakehouses, outputs IDs
│   ├── FM_BC_Notebook_Generator.Notebook   ← scaffolds new module notebooks
│   ├── FM_Utility.Notebook                 ← shared helper functions
│   ├── LakehouseLineage.Notebook
│   ├── Lakehouse deployment scripts.Notebook
│   ├── Optimize lakehouses.Notebook
│   └── T.Environment/                      ← Fabric Spark environment definition
├── Finance Agent.DataAgent/                ← Fabric Data Agent for Finance Q&A
├── Accounts Payable Agent.DataAgent/       ← Fabric Data Agent for Accounts Payable Q&A
├── Accounts Receivable Agent.DataAgent/    ← Fabric Data Agent for Accounts Receivable Q&A
├── FM_BC2ADLS.xml                          ← BC2ADLS export configuration template
└── .github/skills/fabric-module-deploy/    ← Deployment skill
```

## Deploying for a new client

Read the [Deployment Handbook](https://dev.azure.com/fellowminddk/BC%20BI%20standardmodel/_wiki/wikis/Developer%20HandBook/8165/Deployment-Handbook) before deploying for the first time.