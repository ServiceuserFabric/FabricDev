# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Workspace Provisioner
# 
# **Purpose:** Provisions a standard set of Fabric workspaces for a new client deployment, including folder structure, lakehouses, and a Mirrored Database in the Raw Landing Zone.
# 
# **Workspaces created:**
# | Workspace | Contents |
# |---|---|
# | Raw Landing Zone | Mirrored database(s) (GenericMirror / Open Mirroring — source connection configured separately) |
# | Dev / Test / Prod - Dataplatform | Folders: 01 Raw, 02 Data preparation, 03 Enriched, 04 Curated — each with a matching lakehouse |
# | Dev / Test / Prod - Reporting | Empty (reports deployed separately) |
# 
# **Inputs:** `CAPACITY_ID` — the Fabric Capacity GUID to assign all workspaces to. Leave blank to skip capacity assignment.
# 
# **Auth:** Uses the notebook identity via `notebookutils.credentials.getToken` — no credentials required.
# 
# **Required permissions:**
# | Action | Required role |
# |---|---|
# | Create workspaces | Tenant setting **"Create workspaces"** must be enabled for your account (Fabric Admin Portal → Tenant settings) |
# | Create lakehouses & mirrored databases | Automatically granted as workspace Admin (you created it) |
# | Assign workspaces to a capacity | **Capacity Admin** on the target capacity, or **Fabric Admin** (tenant-level) — if missing, capacity assignment is skipped and must be done manually |


# PARAMETERS CELL ********************

# -------------------------------------------------------
# REQUIRED: Fabric Capacity ID (GUID) for all workspaces
# Leave blank to skip capacity assignment
# -------------------------------------------------------
CAPACITY_ID = "17BDC721-F403-4E79-9D0B-ACBAC37320BE"  # e.g. "a1b2c3d4-0000-0000-0000-112233445566"

# -------------------------------------------------------
# OPTIONAL: Prefix added to every workspace name
# Leave empty for no prefix  e.g. "ClientName - "
# -------------------------------------------------------
WORKSPACE_PREFIX = ""

# -------------------------------------------------------
# OPTIONAL: Display name for the mirrored database item
# -------------------------------------------------------
MIRRORED_DB_NAME = "BC2ADLS"

# -------------------------------------------------------
# SELECT: Which environments to provision
# Remove any environments you do not want
# -------------------------------------------------------
ENVIRONMENTS = ["Prod"]  # options: "Dev", "Test", "Prod"

# -------------------------------------------------------
# SELECT: Which workspace types to provision
# -------------------------------------------------------
CREATE_RAW_LANDING_ZONE = True   # Single workspace — not environment-specific
CREATE_DATAPLATFORM      = True  # "[Env] - Dataplatform" per selected environment
CREATE_REPORTING         = True  # "[Env] - Reporting" per selected environment
CREATE_PIPELINES         = False  # Deployment pipelines Dev→Test→Prod (requires >= 2 environments)

# -------------------------------------------------------
# SELECT: Multiple BC environments
# If True, the Raw lakehouse in each Dataplatform workspace is created with
# schemas enabled — required when data from multiple BC environments is loaded
# into separate schemas. WARNING: cannot be changed after lakehouse creation.
# When True, set MIRRORED_DB_COUNT to the number of BC environments (one
# mirrored database per BC environment will be created in the Raw Landing Zone).
# -------------------------------------------------------
MULTIPLE_BC_ENVIRONMENTS = True  # True = Raw lakehouse created with schemas enabled
MIRRORED_DB_COUNT        = 1      # Number of mirrored DBs to create (only used when MULTIPLE_BC_ENVIRONMENTS = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import time
import base64
import json

# Authenticate using the notebook's identity
_token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com/")

BASE_URL = "https://api.fabric.microsoft.com/v1"
HEADERS = {
    "Authorization": f"Bearer {_token}",
    "Content-Type": "application/json",
}


def _post(path: str, body: dict) -> dict:
    """POST to the Fabric REST API. Raises on non-2xx responses."""
    resp = requests.post(f"{BASE_URL}{path}", headers=HEADERS, json=body)
    if not resp.ok:
        raise RuntimeError(f"POST {path} failed [{resp.status_code}]: {resp.text}")
    if resp.status_code == 202:
        return {"status": "accepted", "location": resp.headers.get("Location")}
    return resp.json() if resp.content else {}


def _get(path: str) -> dict:
    """GET from the Fabric REST API. Raises on non-2xx responses."""
    resp = requests.get(f"{BASE_URL}{path}", headers=HEADERS)
    if not resp.ok:
        raise RuntimeError(f"GET {path} failed [{resp.status_code}]: {resp.text}")
    return resp.json() if resp.content else {}


def _poll_until_done(location: str, interval: int = 3, max_wait: int = 120) -> dict:
    """Poll a long-running operation URL until it completes."""
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        resp = requests.get(location, headers=HEADERS)
        data = resp.json()
        status = data.get("status", "").lower()
        if status in ("succeeded", "completed"):
            return data
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Operation failed: {data}")
    raise TimeoutError(f"Operation did not complete within {max_wait}s: {location}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------
# Workspace specification — built from selection above
# -------------------------------------------------------

WORKSPACE_SPEC = []

if CREATE_RAW_LANDING_ZONE:
    WORKSPACE_SPEC.append({
        "name": "Raw Landing Zone",
        "type": "raw_landing_zone",
    })

if CREATE_DATAPLATFORM:
    WORKSPACE_SPEC += [
        {
            "name": f"{env} - Dataplatform",
            "type": "dataplatform",
            "folders": [
                {"folder": "01 Raw",              "lakehouse": "Raw"},
                {"folder": "02 Data preparation", "lakehouse": "DP"},
                {"folder": "03 Enriched",         "lakehouse": "Enr"},
                {"folder": "04 Curated",          "lakehouse": "Cur"},
            ],
        }
        for env in ENVIRONMENTS
    ]

if CREATE_REPORTING:
    WORKSPACE_SPEC += [
        {"name": f"{env} - Reporting", "type": "reporting"}
        for env in ENVIRONMENTS
    ]

print(f"Workspaces queued for provisioning ({len(WORKSPACE_SPEC)}):")
for ws in WORKSPACE_SPEC:
    print(f"  - {WORKSPACE_PREFIX}{ws['name']}")
if MULTIPLE_BC_ENVIRONMENTS:
    print(f"\n⚠  MULTIPLE_BC_ENVIRONMENTS = True — Raw lakehouse will be created with schemas enabled (irreversible).")
    print(f"   {MIRRORED_DB_COUNT} mirrored database(s) will be created in the Raw Landing Zone.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------
# Provisioning helpers
# -------------------------------------------------------

def create_workspace(display_name: str) -> str:
    full_name = f"{WORKSPACE_PREFIX}{display_name}"
    result = _post("/workspaces", {"displayName": full_name})
    workspace_id = result.get("id")
    print(f"  Created workspace: '{full_name}' ({workspace_id})")
    return workspace_id


def assign_to_capacity(workspace_id: str) -> None:
    if not CAPACITY_ID:
        print("⚠  CAPACITY_ID not set — skipping capacity assignment.")
        return
    try:
        result = _post(f"/workspaces/{workspace_id}/assignToCapacity", {"capacityId": CAPACITY_ID})
        if result.get("status") == "accepted" and result.get("location"):
            _poll_until_done(result["location"])
        print(f"  Assigned workspace {workspace_id} to capacity {CAPACITY_ID}")
    except RuntimeError as e:
        if "403" in str(e):
            print("⚠  Capacity assignment skipped — insufficient permissions (403). Assign manually in the Admin Portal.")
        else:
            raise


def create_folder(workspace_id: str, folder_name: str) -> str:
    result = _post(f"/workspaces/{workspace_id}/folders", {"displayName": folder_name})
    folder_id = result.get("id")
    print(f"    Created folder: '{folder_name}' ({folder_id})")
    return folder_id


def create_lakehouse(workspace_id: str, lakehouse_name: str, folder_id: str = None, enable_schemas: bool = False) -> str:
    body = {"displayName": lakehouse_name, "type": "Lakehouse"}
    if folder_id:
        body["folderId"] = folder_id
    if enable_schemas:
        body["creationPayload"] = {"enableSchemas": True}
    result = _post(f"/workspaces/{workspace_id}/items", body)
    item_id = result.get("id")
    schema_note = " [schemas enabled]" if enable_schemas else ""
    print(f"    Created lakehouse: '{lakehouse_name}' ({item_id}){schema_note}")
    return item_id


def get_sql_endpoint_id(workspace_id: str, lakehouse_id: str, max_wait: int = 120) -> tuple:
    """Polls until the SQL Analytics Endpoint for a lakehouse is provisioned.
    Returns (endpoint_id, connection_string)."""
    print(f"    Polling SQL endpoint for lakehouse {lakehouse_id}...")
    elapsed = 0
    interval = 5
    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        data = _get(f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
        sql_props = data.get("properties", {}).get("sqlEndpointProperties", {})
        if sql_props.get("provisioningStatus") == "Success":
            endpoint_id       = sql_props.get("id")
            connection_string = sql_props.get("connectionString")
            print(f"    SQL endpoint ready: {endpoint_id}")
            return endpoint_id, connection_string
    raise TimeoutError(f"SQL endpoint for lakehouse {lakehouse_id} did not provision within {max_wait}s")


def create_mirrored_database(workspace_id: str, name: str) -> str:
    mirroring_def = {
        "properties": {
            "source": {
                "type": "GenericMirror",
                "typeProperties": {}
            },
            "target": {
                "type": "MountedRelationalDatabase",
                "typeProperties": {
                    "defaultSchema": "dbo",
                    "format": "Delta"
                }
            }
        }
    }
    payload = base64.b64encode(json.dumps(mirroring_def).encode()).decode()
    body = {
        "displayName": name,
        "definition": {
            "parts": [
                {
                    "path": "mirroring.json",
                    "payload": payload,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    result = _post(f"/workspaces/{workspace_id}/mirroredDatabases", body)
    item_id = result.get("id")
    print(f"    Created mirrored database: '{name}' ({item_id})")
    print("ℹ  Open Mirroring — configure the source connection in the Fabric UI when ready.")
    return item_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------
# Main provisioning loop
# -------------------------------------------------------

provision_results = []  # workspace-level status for error reporting
ws_registry       = {}  # base_name → workspace_id (used for pipeline stage assignment)
manifest_items    = {}  # base_name → [{type, name, id}]

for spec in WORKSPACE_SPEC:
    ws_name = spec["name"]
    ws_type = spec["type"]
    print(f"\n{'='*60}")
    print(f"Provisioning: {WORKSPACE_PREFIX}{ws_name}  [{ws_type}]")
    print(f"{'='*60}")

    try:
        workspace_id = create_workspace(ws_name)
        assign_to_capacity(workspace_id)
        ws_registry[ws_name]    = workspace_id
        manifest_items[ws_name] = []

        if ws_type == "raw_landing_zone":
            db_names = (
                [f"{MIRRORED_DB_NAME} {i}" for i in range(1, MIRRORED_DB_COUNT + 1)]
                if MULTIPLE_BC_ENVIRONMENTS else
                [MIRRORED_DB_NAME]
            )
            for db_name in db_names:
                item_id = create_mirrored_database(workspace_id, db_name)
                manifest_items[ws_name].append({"type": "MirroredDatabase", "name": db_name, "id": item_id})

        elif ws_type == "dataplatform":
            manifest_items[ws_name].append({"type": "Config", "name": "schemas_enabled", "id": str(MULTIPLE_BC_ENVIRONMENTS).lower()})
            for entry in spec.get("folders", []):
                folder_id  = create_folder(workspace_id, entry["folder"])
                use_schemas = MULTIPLE_BC_ENVIRONMENTS and entry["lakehouse"] == "Raw"
                item_id    = create_lakehouse(workspace_id, entry["lakehouse"], folder_id=folder_id, enable_schemas=use_schemas)
                manifest_items[ws_name].append({"type": "Lakehouse", "name": entry["lakehouse"], "id": item_id})
                if entry["lakehouse"] == "Cur":
                    sql_id, conn_str = get_sql_endpoint_id(workspace_id, item_id)
                    manifest_items[ws_name].append({"type": "SQLEndpoint", "name": "Cur", "id": sql_id, "connection_string": conn_str})

        # "reporting" workspaces are left empty by design

        provision_results.append({"workspace": f"{WORKSPACE_PREFIX}{ws_name}", "status": "OK"})

    except Exception as e:
        print(f"  ERROR: {e}")
        provision_results.append({"workspace": f"{WORKSPACE_PREFIX}{ws_name}", "status": f"FAILED: {e}"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------
# Deployment pipelines
# -------------------------------------------------------

pipeline_results = []

if CREATE_PIPELINES and len(ENVIRONMENTS) >= 2:
    pipeline_configs = []
    if CREATE_DATAPLATFORM:
        pipeline_configs.append(("Dataplatform Pipeline", "Dataplatform"))
    if CREATE_REPORTING:
        pipeline_configs.append(("Reporting Pipeline", "Reporting"))

    for pipeline_name, ws_suffix in pipeline_configs:
        full_name = f"{WORKSPACE_PREFIX}{pipeline_name}"
        print(f"\nCreating pipeline: '{full_name}'")
        try:
            stages_payload = [{"displayName": env} for env in ENVIRONMENTS]
            result = _post("/deploymentPipelines", {"displayName": full_name, "stages": stages_payload})
            pipeline_id = result.get("id")
            print(f"  Created pipeline ({pipeline_id})")

            # Stages are returned in the creation response; fall back to GET if missing
            stages_raw = result.get("stages") or _get(f"/deploymentPipelines/{pipeline_id}/stages").get("value", [])
            stages = sorted(stages_raw, key=lambda s: s["order"])

            for stage in stages:
                stage_order = stage["order"]
                stage_id    = stage["id"]
                env     = ENVIRONMENTS[stage_order]
                ws_name = f"{env} - {ws_suffix}"
                ws_id   = ws_registry.get(ws_name)
                if ws_id:
                    _post(f"/deploymentPipelines/{pipeline_id}/stages/{stage_id}/assignWorkspace", {"workspaceId": ws_id})
                    print(f"  Stage {stage_order} ({env}): assigned '{ws_name}'")
                else:
                    print(f"  ⚠  Stage {stage_order} ({env}): '{ws_name}' not in registry — skipped.")

            pipeline_results.append({"name": pipeline_name, "id": pipeline_id, "status": "OK"})

        except Exception as e:
            print(f"  ERROR: {e}")
            pipeline_results.append({"name": pipeline_name, "id": None, "status": f"FAILED: {e}"})

elif not CREATE_PIPELINES:
    print("Deployment pipelines skipped (CREATE_PIPELINES = False).")
else:
    print(f"Deployment pipelines skipped — need >= 2 environments, got {len(ENVIRONMENTS)}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------
# Summary
# -------------------------------------------------------

failed_ws = [r for r in provision_results if r["status"] != "OK"]
print(f"Workspaces: {len(provision_results) - len(failed_ws)}/{len(provision_results)} provisioned successfully.")
if failed_ws:
    print(f"  Failed: {[r['workspace'] for r in failed_ws]}")

if pipeline_results:
    failed_p = [p for p in pipeline_results if p["status"] != "OK"]
    print(f"Pipelines:  {len(pipeline_results) - len(failed_p)}/{len(pipeline_results)} created successfully.")

# YAML manifest
print("\n" + "="*50)
print("PROVISIONED MANIFEST")
print("="*50)
print("workspaces:\n")

for spec in WORKSPACE_SPEC:
    ws_name = spec["name"]
    ws_id   = ws_registry.get(ws_name, "FAILED")
    items   = manifest_items.get(ws_name, [])
    print(f"  {WORKSPACE_PREFIX}{ws_name}:")
    print(f"    workspace_id: {ws_id}")
    if items:
        config_items = [i for i in items if i["type"] == "Config"]
        list_items   = [i for i in items if i["type"] != "Config"]
        for cfg in config_items:
            print(f"    {cfg['name']}: {cfg['id']}")
        if list_items:
            print("    items:")
            for item in list_items:
                print(f"      - type: {item['type']}")
                print(f"        name: {item['name']}")
                print(f"        id:   {item['id']}")
                if item.get("connection_string"):
                    print(f"        connection_string: {item['connection_string']}")
    else:
        print("    items: []")
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
