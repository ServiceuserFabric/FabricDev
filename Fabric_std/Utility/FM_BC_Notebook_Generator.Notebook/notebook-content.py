# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "31a507aa-9055-4731-872a-f978422b690b",
# META       "default_lakehouse_name": "Raw",
# META       "default_lakehouse_workspace_id": "706a6743-3c4a-44ff-ab89-b43d6a73607a",
# META       "known_lakehouses": [
# META         {
# META           "id": "31a507aa-9055-4731-872a-f978422b690b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: Imports
from notebookutils import mssparkutils
from pyspark.sql.functions import concat_ws, col, lit, current_timestamp
import json
from datetime import datetime
import requests
import base64

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Configuration
SOURCE_LAKEHOUSE = "Raw"
ENRICHED_NOTEBOOK_PATH = "/03 Enriched/Notebooks"
CURATED_NOTEBOOK_PATH = "/04 Curated/Notebooks"
TRACKING_TABLE = "notebook_generation_tracking"

# Define your key columns list (edit as needed)
# This is a master list - notebooks will only create keys for columns that exist in each table
KEY_COLUMNS = ['VendorNo', 'CustomerNo', 'ItemNo', 'OrderID']  # Add all possible key columns here

# API Integration Settings
USE_API_CREATION = True  # Set to False to use file-based approach instead

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: API Functions
def get_fabric_access_token():
    """Get access token using the notebook's context"""
    try:
        token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
        return token
    except Exception as e:
        print(f"⚠️ Error getting token: {e}")
        return None

# def get_workspace_id():
#     """Get the current workspace ID"""
#     try:
#         workspace_id = mssparkutils.env.getWorkspaceId()
#         return workspace_id
#     except Exception as e:
#         print(f"⚠️ Error getting workspace ID: {e}")
#         return None

def get_workspace_id():
    """Get the current workspace ID"""
    # Replace with your actual workspace ID from the URL
    return "706a6743-3c4a-44ff-ab89-b43d6a73607a" 

def create_fabric_notebook_via_api(notebook_name, notebook_content, workspace_id=None):
    """Create a notebook directly in Fabric using REST API"""
    
    if workspace_id is None:
        workspace_id = get_workspace_id()
    
    token = get_fabric_access_token()
    
    if not token or not workspace_id:
        print("    ❌ Could not authenticate or get workspace ID")
        return False
    
    # Fabric API endpoint
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Convert notebook content to base64
    notebook_json = json.dumps(notebook_content)
    encoded_content = base64.b64encode(notebook_json.encode()).decode()
    
    payload = {
        "displayName": notebook_name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": encoded_content,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code in [200, 201, 202]:
            print(f"    ✅ Notebook '{notebook_name}' created successfully in workspace!")
            return True
        else:
            print(f"    ❌ API failed (Status {response.status_code}): {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"    ❌ Error calling API: {str(e)}")
        return False



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Supporting Functions
def initialize_tracking_table():
    try:
        spark.sql(f"SELECT * FROM {TRACKING_TABLE} LIMIT 1")
    except:
        print(f"Creating tracking table '{TRACKING_TABLE}'")
        tracking_schema = """
            CREATE TABLE {tracking_table} (
                table_name STRING,
                enriched_notebook_created BOOLEAN,
                curated_notebook_created BOOLEAN,
                created_timestamp TIMESTAMP,
                last_updated TIMESTAMP
            ) USING DELTA
        """.format(tracking_table=TRACKING_TABLE)
        spark.sql(tracking_schema)

def get_processed_tables():
    try:
        processed = spark.sql(f"""
            SELECT table_name 
            FROM {TRACKING_TABLE} 
            WHERE enriched_notebook_created = true 
            AND curated_notebook_created = true
        """).collect()
        return {row.table_name for row in processed}
    except:
        return set()

def mark_table_as_processed(table_name, enriched_created, curated_created):
    from pyspark.sql import Row
    
    existing = spark.sql(f"""
        SELECT * FROM {TRACKING_TABLE} 
        WHERE table_name = '{table_name}'
    """).collect()
    
    if existing:
        spark.sql(f"""
            UPDATE {TRACKING_TABLE}
            SET enriched_notebook_created = {enriched_created},
                curated_notebook_created = {curated_created},
                last_updated = current_timestamp()
            WHERE table_name = '{table_name}'
        """)
    else:
        new_record = spark.createDataFrame([
            Row(
                table_name=table_name,
                enriched_notebook_created=enriched_created,
                curated_notebook_created=curated_created,
                created_timestamp=datetime.now(),
                last_updated=datetime.now()
            )
        ])
        new_record.write.mode("append").saveAsTable(TRACKING_TABLE)

def clean_table_name(table_name):
    """Remove BC2ADLS suffix from table names (e.g., 'Vendor-23' -> 'Vendor')"""
    parts = table_name.rsplit('-', 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    return table_name

def get_table_columns(lakehouse, table_name):
    """Get column names and types from a table"""
    try:
        # Escape table name with backticks to handle special characters like hyphens
        df = spark.table(f'`{lakehouse}`.`{table_name}`')
        columns = [(field.name, str(field.dataType)) for field in df.schema.fields]
        return columns
    except Exception as e:
        print(f"Warning: Could not get columns for {table_name}: {e}")
        return []

def create_enriched_notebook(lakehouse, table_name, key_columns, display_name=None):
    """Generate enriched notebook with dynamic column selection
    
    Args:
        lakehouse: Source lakehouse name (e.g., 'Raw')
        table_name: Original table name with suffix (e.g., 'Vendor-23') - used for reading
        key_columns: List of key column names to generate keys for
        display_name: Clean name without suffix (e.g., 'Vendor') - used for display and writing
    """
    
    if display_name is None:
        display_name = table_name
    
    # Get actual columns from the table using original name
    table_columns = get_table_columns(lakehouse, table_name)
    
    # Generate column select statements
    if table_columns:
        select_statements = [f"    col('{col_name}')," for col_name, col_type in table_columns]
        select_code = "\n".join(select_statements)
        # Remove trailing comma from last line
        select_code = select_code.rstrip(',')
    else:
        select_code = "    # Could not retrieve columns - add them manually"
    
    # Generate key column code
    key_columns_list = ", ".join([f"'{col}'" for col in key_columns])
    
    notebook_cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# Enriched: {display_name}\n\n",
                f"*Generated by: FM_BC_Notebook_Generator*\n\n",
                f"This notebook processes data from the Raw layer and creates the enriched version with standardized key columns and transformations.\n\n",
                f"**Source:** {lakehouse}.{table_name}  \n",
                f"**Target:** Enr.{display_name}"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Standard imports for data processing\n",
                "from pyspark.sql.functions import col, concat_ws, lit, current_timestamp, to_date, trim, upper, lower\n",
                "from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType\n",
                "from datetime import datetime\n",
                "import pandas as pd"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "%run FM_Utility"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Read from Raw layer\n",
                f"df = spark.table('`{lakehouse}`.`{table_name}`')\n\n",
                f"print(f\"✓ Loaded {{df.count():,}} rows from {lakehouse}.{table_name}\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Select columns (comment out ones you don't need)\n",
                f"df_selected = df.select(\n",
                f"{select_code}\n",
                f")\n\n",
                f"print(f\"Selected {{len(df_selected.columns)}} columns\")\n",
                f"display(df_selected.limit(5))"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Key columns configuration (maintained in generator)\n",
                f"KEY_COLUMNS = [{key_columns_list}]\n\n",
                f"# Get columns that actually exist in this table\n",
                f"existing_columns = df_selected.columns\n",
                f"applicable_key_columns = [col for col in KEY_COLUMNS if col in existing_columns]\n\n",
                f"# Create composite key columns only for existing columns\n",
                f"if applicable_key_columns:\n",
                f"    df_enriched = df_selected\n",
                f"    print(f\"Creating keys for: {{', '.join(applicable_key_columns)}}\")\n",
                f"    \n",
                f"    for key_col in applicable_key_columns:\n",
                f"        key_column_name = f'{{key_col}}_Key'\n",
                f"        df_enriched = df_enriched.withColumn(\n",
                f"            key_column_name,\n",
                f"            concat_ws('_', col('companyKey'), col(key_col))\n",
                f"        )\n",
                f"        print(f\"  ✓ Created: {{key_column_name}}\")\n",
                f"    \n",
                f"    # Show which columns were skipped\n",
                f"    skipped_columns = [col for col in KEY_COLUMNS if col not in existing_columns]\n",
                f"    if skipped_columns:\n",
                f"        print(f\"\\n⚠️ Skipped (not in table): {{', '.join(skipped_columns)}}\")\n",
                f"    \n",
                f"    # Show sample keys\n",
                f"    print(\"\\nSample Key values:\")\n",
                f"    key_columns_to_show = [f'{{c}}_Key' for c in applicable_key_columns]\n",
                f"    display(df_enriched.select('companyKey', *applicable_key_columns, *key_columns_to_show).limit(10))\n",
                f"else:\n",
                f"    df_enriched = df_selected\n",
                f"    print(\"⚠️ No applicable key columns found in this table\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Add your custom transformations here\n",
                f"# Example: Add load timestamp, clean data, etc.\n\n",
                f"df_enriched = df_enriched.withColumn('LoadTimestamp', current_timestamp())\n\n",
                f"print(f\"✓ Enriched dataset ready: {{df_enriched.count():,}} rows, {{len(df_enriched.columns)}} columns\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Write to Enriched layer\n",
                f"target_table = 'Enr.{display_name}'\n\n",
                f"df_enriched.write.mode('overwrite').format('delta').saveAsTable(target_table)\n\n",
                f"print(f\"✓ Successfully written to {{target_table}}\")\n",
                f"print(f\"  Rows written: {{df_enriched.count():,}}\")\n",
                f"print(f\"  Columns: {{len(df_enriched.columns)}}\")"
            ]
        }
    ]
    
    return {
        "cells": notebook_cells,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {
                "name": "synapse_pyspark",
                "display_name": "synapse_pyspark"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

def create_curated_notebook(table_name, display_name=None):
    """Generate curated notebook with dynamic column selection
    
    Args:
        table_name: Original table name with suffix (e.g., 'Vendor-23') - used for reading
        display_name: Clean name without suffix (e.g., 'Vendor') - used for display and writing
    """
    
    if display_name is None:
        display_name = table_name
    
    # Get actual columns from the enriched table (fallback to empty if doesn't exist yet)
    table_columns = get_table_columns('Enr', display_name)
    
    # Generate column select statements
    if table_columns:
        select_statements = [f"    col('{col_name}')," for col_name, col_type in table_columns]
        select_code = "\n".join(select_statements)
        # Remove trailing comma from last line
        select_code = select_code.rstrip(',')
    else:
        # Fallback: try to get from Raw table with original name
        table_columns = get_table_columns('Raw', table_name)
        if table_columns:
            select_statements = [f"    col('{col_name}')," for col_name, col_type in table_columns]
            select_code = "\n".join(select_statements)
            select_code = select_code.rstrip(',')
        else:
            select_code = "    # Could not retrieve columns - add them manually"
    
    notebook_cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# Curated: {display_name}\n\n",
                f"*Generated by: FM_BC_Notebook_Generator*\n\n",
                f"This notebook processes data from the Enriched layer and creates the curated version ready for reporting and analytics.\n\n",
                f"**Source:** Enr.{display_name}  \n",
                f"**Target:** Cur.{display_name}"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Standard imports for data processing\n",
                "from pyspark.sql.functions import col, concat_ws, lit, current_timestamp, to_date, trim, upper, lower\n",
                "from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType\n",
                "from datetime import datetime\n",
                "import pandas as pd"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "%run FM_Utility"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Read from Enriched layer\n",
                f"df = spark.table('`Enr`.`{display_name}`')\n\n",
                f"print(f\"✓ Loaded {{df.count():,}} rows from Enr.{display_name}\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Select columns (comment out ones you don't need)\n",
                f"df_selected = df.select(\n",
                f"{select_code}\n",
                f")\n\n",
                f"print(f\"Selected {{len(df_selected.columns)}} columns\")\n",
                f"display(df_selected.limit(5))"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Add your curated layer transformations here\n",
                f"# Example: Apply business rules, aggregations, final column selection, etc.\n\n",
                f"df_curated = df_selected  # Customize as needed\n\n",
                f"print(f\"✓ Curated dataset ready: {{df_curated.count():,}} rows, {{len(df_curated.columns)}} columns\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"# Write to Curated layer\n",
                f"target_table = 'Cur.{display_name}'\n\n",
                f"df_curated.write.mode('overwrite').format('delta').saveAsTable(target_table)\n\n",
                f"print(f\"✓ Successfully written to {{target_table}}\")\n",
                f"print(f\"  Rows written: {{df_curated.count():,}}\")\n",
                f"print(f\"  Columns: {{len(df_curated.columns)}}\")"
            ]
        }
    ]
    
    return {
        "cells": notebook_cells,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {
                "name": "synapse_pyspark",
                "display_name": "synapse_pyspark"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

def create_notebook(notebook_name, folder_path, content):
    """Create notebook - tries API first, falls back to file save"""
    
    if USE_API_CREATION:
        # Try API creation first
        print(f"    🔄 Attempting to create via Fabric API...")
        success = create_fabric_notebook_via_api(notebook_name, content)
        
        if success:
            return True
        else:
            print(f"    ⚠️ API creation failed, falling back to file save...")
    
    # Fallback: Save to files
    try:
        notebook_json = json.dumps(content, indent=2)
        file_path = f"Files/generated_notebooks/{notebook_name}.ipynb"
        mssparkutils.fs.put(file_path, notebook_json, True)
        print(f"    💾 Notebook saved to: {file_path}")
        return True
    except Exception as e:
        print(f"    ❌ Error creating notebook {notebook_name}: {str(e)}")
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Preview Function
def preview_new_tables():
    """Shows which tables will be processed WITHOUT creating anything"""
    
    # Initialize tracking table if needed
    initialize_tracking_table()
    
    # Get all current tables
    all_tables = spark.sql(f"SHOW TABLES IN {SOURCE_LAKEHOUSE}").collect()
    all_table_names = {row.tableName for row in all_tables}
    
    # Get already processed tables
    processed_tables = get_processed_tables()
    
    # Find NEW tables only
    new_tables = all_table_names - processed_tables
    
    print("=" * 70)
    print("PREVIEW: New Tables Detection")
    print("=" * 70)
    print(f"\nTotal tables in lakehouse: {len(all_table_names)}")
    print(f"Already processed tables: {len(processed_tables)}")
    print(f"New tables found: {len(new_tables)}")
    print(f"Creation method: {'🌐 Fabric REST API' if USE_API_CREATION else '💾 File-based'}")
    
    if len(new_tables) == 0:
        print("\n✓ No new tables to process!")
        print("\nAll existing tables have notebooks already created.")
    else:
        print(f"\n📋 The following {len(new_tables)} table(s) will have notebooks created:")
        print("-" * 70)
        for i, table_name in enumerate(sorted(new_tables), 1):
            print(f"  {i}. {table_name}")
            print(f"      → Will create: ENR_{table_name}")
            print(f"      → Will create: CUR_{table_name}")
        
        print("\n" + "=" * 70)
        print("📝 To proceed with notebook creation, run: create_notebooks(confirm=True)")
        print("=" * 70)
    
    # Show already processed tables
    if len(processed_tables) > 0:
        print(f"\n✓ Already processed ({len(processed_tables)} tables):")
        for table_name in sorted(processed_tables):
            print(f"  - {table_name}")
    
    return new_tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Create Notebooks Function
def create_notebooks(confirm=False):
    """Actually creates the notebooks for new tables"""
    
    if not confirm:
        print("⚠️  You must set confirm=True to proceed with notebook creation")
        print("Example: create_notebooks(confirm=True)")
        return
    
    # Initialize tracking table if needed
    initialize_tracking_table()
    
    # Get all current tables
    all_tables = spark.sql(f"SHOW TABLES IN {SOURCE_LAKEHOUSE}").collect()
    all_table_names = {row.tableName for row in all_tables}
    
    # Get already processed tables
    processed_tables = get_processed_tables()
    
    # Find NEW tables only
    new_tables = all_table_names - processed_tables
    
    if len(new_tables) == 0:
        print("No new tables to process!")
        return
    
    print("=" * 70)
    print(f"Creating notebooks for {len(new_tables)} new table(s)...")
    print(f"Method: {'🌐 Fabric REST API' if USE_API_CREATION else '💾 File-based'}")
    print("=" * 70)
    
    success_count = 0
    failure_count = 0
    
    # Process each NEW table
    for i, table_name in enumerate(sorted(new_tables), 1):
        print(f"\n[{i}/{len(new_tables)}] Processing: {table_name}")
        print("-" * 70)
        
        try:
            # Create Enriched Notebook
            table_name_clean = clean_table_name(table_name)
            enriched_notebook_name = f"ENR_{table_name_clean}"
            print(f"  📓 Creating enriched notebook...")
            enriched_content = create_enriched_notebook(SOURCE_LAKEHOUSE, table_name, KEY_COLUMNS, table_name_clean)
            enriched_created = create_notebook(enriched_notebook_name, ENRICHED_NOTEBOOK_PATH, enriched_content)

            
            # Create Curated Notebook
            curated_notebook_name = f"CUR_{table_name_clean}"
            print(f"  📓 Creating curated notebook...")
            curated_content = create_curated_notebook(table_name, table_name_clean)
            curated_created = create_notebook(curated_notebook_name, CURATED_NOTEBOOK_PATH, curated_content)
            
            # Mark as processed
            mark_table_as_processed(table_name, enriched_created, curated_created)
            
            if enriched_created and curated_created:
                print(f"✅ Successfully created notebooks for: {table_name}")
                success_count += 1
            else:
                print(f"⚠️ Partially created notebooks for: {table_name}")
                success_count += 1
            
        except Exception as e:
            print(f"❌ Error processing {table_name}: {str(e)}")
            mark_table_as_processed(table_name, False, False)
            failure_count += 1
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {failure_count}")
    print(f"📊 Total processed: {success_count + failure_count}")
    
    if not USE_API_CREATION:
        print("\n💡 Notebooks saved to Files/generated_notebooks/")
        print("   Import them manually from your lakehouse Files folder")
    
    # Show tracking status
    print("\nCurrent tracking status:")
    display(spark.sql(f"SELECT * FROM {TRACKING_TABLE} ORDER BY last_updated DESC"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7: Helper Functions
def initialize_existing_tables():
    """Mark all current tables as already processed - run this ONCE on first setup"""
    
    initialize_tracking_table()
    
    # Get all current tables
    all_tables = spark.sql(f"SHOW TABLES IN {SOURCE_LAKEHOUSE}").collect()
    all_table_names = [row.tableName for row in all_tables]
    
    print(f"Found {len(all_table_names)} existing tables")
    print("Marking them all as already processed...")
    
    from pyspark.sql import Row
    
    records = []
    for table_name in all_table_names:
        records.append(
            Row(
                table_name=table_name,
                enriched_notebook_created=True,
                curated_notebook_created=True,
                created_timestamp=datetime.now(),
                last_updated=datetime.now()
            )
        )
    
    if records:
        df = spark.createDataFrame(records)
        df.write.mode("append").saveAsTable(TRACKING_TABLE)
        print(f"✅ Marked {len(records)} tables as already processed")
    
    # Show what was added
    print("\nTracking table contents:")
    display(spark.sql(f"SELECT * FROM {TRACKING_TABLE}"))

def delete_table_from_tracking(*table_names):
    """Delete one or more tables from tracking (for re-generation)"""
    
    # Handle empty call
    if not table_names:
        print("⚠️ No tables specified")
        return
    
    for name in table_names:
        spark.sql(f"""
            DELETE FROM {TRACKING_TABLE}
            WHERE table_name = '{name}'
        """)
        print(f"✅ Deleted '{name}' from tracking table")
    
    print("\nRemaining records:")
    display(spark.sql(f"SELECT * FROM {TRACKING_TABLE} ORDER BY table_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8: Quick Commands
print("=" * 70)
print("FABRIC NOTEBOOK GENERATOR - Ready!")
print("=" * 70)
print("\n📋 Available Commands:")
print("  1. preview_new_tables()                    - Preview which tables will be processed")
print("  2. create_notebooks(confirm=True)          - Create notebooks for new tables")
print("  3. initialize_existing_tables()            - Mark all current tables as processed (first-time setup)")
print("  4. delete_table_from_tracking('TableName') - Remove table(s) from tracking to regenerate")
print("\n⚙️ Settings:")
print(f"  - Source Lakehouse: {SOURCE_LAKEHOUSE}")
print(f"  - API Creation: {'✅ Enabled' if USE_API_CREATION else '❌ Disabled (using file-based)'}")
print(f"  - Key Columns: {KEY_COLUMNS if KEY_COLUMNS else 'None configured'}")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delete_table_from_tracking('Vendor-23', 'Job-167')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

preview_new_tables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

create_notebooks(confirm=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
