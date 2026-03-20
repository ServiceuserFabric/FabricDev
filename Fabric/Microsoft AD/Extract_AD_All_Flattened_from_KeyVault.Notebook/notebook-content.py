# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9b2f31d6-7763-4e3f-a1dd-929063e067b3",
# META       "default_lakehouse_name": "MicrosoftAD",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "9b2f31d6-7763-4e3f-a1dd-929063e067b3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# If needed in your environment, uncomment:
# %pip install msal requests

import json
import requests
import msal
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------
# CONFIGURATION
# ---------------------------
# Key Vault configuration
KV_VAULT_NAME = "keyvaultaltibox"
KEY_VAULT_URL = "https://keyvaultaltibox.vault.azure.net"  # <-- TODO: replace with your vault URL
SECRET_NAME_CLIENT_ID = "3d3e62ee-480e-4d03-8373-8f6a812e4c1f"
SECRET_NAME_CLIENT_SECRET = "GraphClientSecret"

# Try this if your environment expects a Key Vault *linked service name* (common in Synapse):
KV_LINKED_SERVICE_NAME = "KeyVaultLinkedService"  # <-- replace with your actual linked service name


# Graph / Tenant configuration (tenant fixed in your input)
tenant_id = "0b29d0fd-ac3d-464a-b671-15011c6682c5"
scope = ["https://graph.microsoft.com/.default"]
base_url = "https://graph.microsoft.com/v1.0/"

# Lakehouse tables (Fabric)
USERS_TABLE_PATH = "Tables/MicrosoftAD_users"
GROUPS_TABLE_PATH = "Tables/MicrosoftAD_groups"
MEMBERSHIPS_TABLE_PATH = "Tables/MicrosoftAD_group_memberships"

spark = SparkSession.builder.getOrCreate()


# Cell 2 — Retrieve the secret (tries linked service first, then vault name)
from notebookutils import mssparkutils  # available in Synapse/Fabric runtimes

client_secret = None
errors = []


# Attempt 1: using linked service name
try:
    client_secret = mssparkutils.credentials.getSecret(KV_LINKED_SERVICE_NAME, SECRET_NAME_CLIENT_SECRET)
except Exception as e:
    errors.append(f"Linked service fetch failed: {e}")


if not client_secret:
    raise RuntimeError(
        "Failed to retrieve secret from Key Vault. "
        f"Tried vault '{KV_VAULT_NAME}'. "
        f"Errors: {errors}"
    )


authority = f"https://login.microsoftonline.com/{tenant_id}"
app = msal.ConfidentialClientApplication(SECRET_NAME_CLIENT_ID, authority=authority, client_credential=client_secret)
token_result = app.acquire_token_for_client(scopes=scope)

if "access_token" not in token_result:
    raise Exception(f"Failed to obtain access token. Details: {token_result}")

access_token = token_result["access_token"]
headers = {
    "Authorization": f"Bearer {access_token}",
    # ConsistencyLevel is useful if you later add $count or advanced filters; harmless to keep here
    "ConsistencyLevel": "eventual"
}

# ---------------------------
# Helpers: paging + flattening
# ---------------------------
def fetch_all(url: str, headers: dict, max_pages: int = 1000):
    """Follow @odata.nextLink to collect all pages."""
    data = []
    page_count = 0
    while url and page_count < max_pages:
        resp = requests.get(url, headers=headers)
        if not resp.ok:
            raise Exception(f"Graph call failed: {resp.status_code} - {resp.text}")
        payload = resp.json()
        data.extend(payload.get("value", []))
        url = payload.get("@odata.nextLink")
        page_count += 1
    return data

def stringify_kv(d: dict) -> str:
    """Convert a dict to 'k1=v1;k2=v2' string for column-safe representation."""
    parts = []
    for k, v in d.items():
        # turn nested values into simple strings
        if isinstance(v, (dict, list)):
            v = json.dumps(v, separators=(',',':'))  # compact; rare fallback
        parts.append(f"{k}={'' if v is None else str(v)}")
    return ";".join(parts)

def flatten_record(obj, parent_key="", sep="."):
    """
    Flatten nested dicts into 'parent.child' columns.
    Lists:
      - list of primitives -> 'a|b|c'
      - list of dicts -> 'k1=v1;k2=v2 | k1=v1;k2=v2' (pipe between items)
    Everything becomes a string (or None), which keeps schema columnar.
    """
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_record(v, new_key, sep))
            elif isinstance(v, list):
                if all(not isinstance(x, (dict, list)) for x in v):
                    # primitives
                    items[new_key] = "|".join("" if x is None else str(x) for x in v)
                else:
                    # list of dicts or mixed -> join each item stringified
                    joined = " | ".join(stringify_kv(x) if isinstance(x, dict) else ("" if x is None else str(x)) for x in v)
                    items[new_key] = joined
            else:
                items[new_key] = None if v is None else str(v)
    else:
        items[parent_key or "value"] = None if obj is None else str(obj)
    return items

def to_flat_df(records):
    """Convert list[dict] records -> Spark DataFrame with union of all flattened keys as string columns."""
    flat_rows = [flatten_record(r) for r in records]
    # union of all keys across rows
    all_cols = sorted({k for row in flat_rows for k in row.keys()})
    schema = StructType([StructField(c, StringType(), True) for c in all_cols])
    # align row values to column order
    data_rows = [tuple(row.get(c) for c in all_cols) for row in flat_rows]
    return spark.createDataFrame(data_rows, schema)

# ---------------------------
# 1) Users (all attributes)
# ---------------------------
users_url = base_url + "users"  # no $select -> full objects (subject to permissions)
users = fetch_all(users_url, headers)
df_users = to_flat_df(users)

# ---------------------------
# 2) Groups (all attributes)
# ---------------------------
groups_url = base_url + "groups"
groups = fetch_all(groups_url, headers)
df_groups = to_flat_df(groups)

# ---------------------------
# 3) Memberships (full member objects, plus relationship cols)
# ---------------------------
memberships_records = []
for g in groups:
    gid = g.get("id")
    gname = g.get("displayName", "")
    if not gid:
        continue
    members_url = base_url + f"groups/{gid}/members"  # returns directoryObject items (users, groups, service principals, etc.)
    members = fetch_all(members_url, headers)
    for m in members:
        flat_member = flatten_record(m)
        # add relationship columns
        flat_member["relationship.groupId"] = gid
        flat_member["relationship.groupDisplayName"] = gname
        memberships_records.append(flat_member)

# Because membership rows come as flattened dicts already, we build the union schema and DataFrame
all_membership_cols = sorted({k for r in memberships_records for k in r.keys()})
membership_schema = StructType([StructField(c, StringType(), True) for c in all_membership_cols])
membership_rows = [tuple(r.get(c) for c in all_membership_cols) for r in memberships_records]
df_memberships = spark.createDataFrame(membership_rows, membership_schema)

# ---------------------------
# 4) Write to Fabric Lakehouse tables (Delta)
# ---------------------------
df_users.write.format("delta").mode("overwrite").save(USERS_TABLE_PATH)
df_groups.write.format("delta").mode("overwrite").save(GROUPS_TABLE_PATH)
df_memberships.write.format("delta").mode("overwrite").save(MEMBERSHIPS_TABLE_PATH)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
