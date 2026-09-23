# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Update lakehouse definitions on publication
# WorkItem: -
# 
# #### workflow
# - Creates shortcuts to target enviroment same
# - Cheks target enviroments lakehouse tables and incase some tables are missing initializes empty tables
# This only creates the <strong> missing </strong> tables when run at enviroment != dev. 

# CELL ********************

import requests
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class env_id:
    """Replace the dev, test, prod strings with the proper GUID it can be found from the URL: 
    https://app.fabric.microsoft.com/groups/<workspace id>/synapsenotebooks/----"""
    dev = '79e0cc15-eecd-4160-b059-df1a81b85adc'
    test = 'c8c93faa-1f2e-41ff-b631-f212a6cc68e8'
    prod = 'bfe1a35b-60b6-426b-a580-3709c9aac765'
    raw = '667aec2a-5aca-41c5-aafd-7d8e3daf2ab1'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_env_id (env):
    """Returns workspace ID of different workspaces based on name
    Args:
        env (str): Name of the workspace
    Returns: guid (str): GUID of the workspace

    example: get_env_id('dev') returns the GUID of the dev workspace
    """
    if env == 'dev':
        id = env_id.dev
    elif env == 'test':
        id = env_id.test
    elif env == 'prod':
        id = env_id.prod
    elif env == 'raw':
        id = env_id.raw
    else:
        id = ''
    return id
    
def get_abfs_path(lakehouse, environment=''):
    """
    Returns ABFSS path to desired lakehouse, if no enviroment given then returns runtimes enviroment.
    arguments:
    Lakehouse name
    optional: enviroment as string: dev, test, prod"""

    environment = get_env_id(environment)

    lh = notebookutils.lakehouse.get(lakehouse,environment)
    abfs = lh.properties['abfsPath']
    return abfs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Function to create shorcuts just like from source enviroment to target enviroment. 
def get_shortcut_tables(env,lakehouse_name):
    """
    env = dev,test,prod
    lakehouse_name = name of the lakehouse where to get the links. 

    TODO: paginate this incase there are large amount of shortcuts
    """
    workspaceId = get_env_id(env)
    lakehouseId = get_abfs_path(lakehouse_name).split('/')[-1]
    api_url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{lakehouseId}/shortcuts'
    
    token = notebookutils.credentials.getToken('https://api.fabric.microsoft.com/')

    header = {'Content-Type':'application/json','Authorization':f'Bearer {token}'}

    resp = requests.get(url=api_url, headers = header)
    return resp.json().get('value'),token

def create_onelake_shortcut_tables(source_env,target_env,lakehouse_name_source,lakehouse_name_target):
    """
    Generates shortcuts as they are in source enviroment to target enviroment. Only creates shortcuts that are pointing to onelake!
    Tries to recreate every link, but becouse same name already exist it will abort. 
    returns the apis statuses as list. 
    Example
    resp = create_linked_onelake_tables('dev','test','Raw','Raw')
    """

    linked_tables, token = get_shortcut_tables(source_env,lakehouse_name_source)
    workspaceId = get_env_id(target_env)
    lakehouseId = get_abfs_path(lakehouse_name_target,target_env).split('/')[-1]

    api_url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{lakehouseId}/shortcuts'
    header = {'Content-Type':'application/json','Authorization':f'Bearer {token}'}
    
    one_lake_tables = []
    resps = []
    #Find oneLake tables
    for table in linked_tables:
        #print(table.get('target').get('type'))
        if table.get('target').get('type') == 'OneLake':
            one_lake_tables.append(table)
    #print(one_lake_tables)
    for table in one_lake_tables:
        print("creating shortcut")
        body = json.dumps(table)
        resp = requests.post(url=api_url,headers = header, data = body)
        resps.append(resp)
    return resp


def create_onelake_shortcut_tables_inside_workspace(source_env,target_env,lakehouse_name_source,lakehouse_name_target,target_shortcut_lakehouse):
    """
    Generates shortcuts that are in source enviroment to target enviroment, only works if there are only inter workspace links like linking from raw to silver. 
    Only creates shortcuts that are pointing to onelake!
    Tries to recreate every link, but incase same name already exist api will abort. 
    returns the apis statuses as list. 
    target_shortcut_lakehouse - the lakehouse the wanted shortcuts are made to .
    TODO: dynamically check the original lakehouse and by name get the right path! (incase from multiple sources)
    Example
    resp = create_linked_onelake_tables('dev','test','Raw','Raw')
    """

    linked_tables, token = get_shortcut_tables(source_env,lakehouse_name_source)
    workspaceId = get_env_id(target_env)
    lakehouseId = get_abfs_path(lakehouse_name_target,target_env).split('/')[-1]

    api_url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{lakehouseId}/shortcuts'
    header = {'Content-Type':'application/json','Authorization':f'Bearer {token}'}
    
    one_lake_tables = []
    resps = []
    #Find oneLake tables
    for table in linked_tables:
        #print(table.get('target').get('type'))
        if table.get('target').get('type') == 'OneLake':
            one_lake_tables.append(table)
    for table in one_lake_tables:
        table['target']['oneLake']['itemId'] = target_shortcut_lakehouse
        table['target']['oneLake']['workspaceId'] = workspaceId
    for table in one_lake_tables:
        print("creating shortcut")
        body = json.dumps(table)
        resp = requests.post(url=api_url,headers = header, data = body)
        resps.append(resp)
    return resp
    #print(one_lake_tables)

#Example
#resp = create_linked_onelake_tables('dev','test','Raw','Raw')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to create tables
def get_create_table_statement(table_name,abfss_path):
    df = spark.read.format("delta").load(abfss_path)  # Adjust format if needed (e.g., "csv", "json")
    schema = df.schema
    columns = []
    for field in schema.fields:
        columns.append(f"{field.name} {field.dataType.simpleString()}")
    columns_str = ", ".join(columns)
    create_table_statement = f"CREATE TABLE {table_name} ({columns_str});"
    return create_table_statement

#get_create_table_statement('test','abfss://858e71de-a254-4a6b-9ea6-ccc5c5953259@onelake.dfs.fabric.microsoft.com/0986d24c-83bb-4437-9d51-14bfb50d0a29/Tables/enheter')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to create the table in target enviroment
def create_table_target(table_name, abfss_path, target_lakehouse_abfss_path):
    df = spark.read.format("delta").load(abfss_path)  # Adjust format if needed (e.g., "csv", "json")
    df = df.limit(0)
    df.write.mode('overwrite').format('delta').save(f"{target_lakehouse_abfss_path}/Tables/{table_name}")
    print(f'created table {table_name}')
    return 0

#get_create_table_statement('test','abfss://858e71de-a254-4a6b-9ea6-ccc5c5953259@onelake.dfs.fabric.microsoft.com/0986d24c-83bb-4437-9d51-14bfb50d0a29/Tables/enheter')

def array_difference(array1, array2):
    set1 = set(array1)
    set2 = set(array2)
    difference = set1 - set2
    return list(difference)

def create_missing_tables():
    dev_env = env_id.dev
    dev_lh = notebookutils.lakehouse.list(dev_env)
    dev_lakenames = [lake['displayName'] for lake in dev_lh]

    lh_cur = notebookutils.lakehouse.list()
    cur_lakenames = [lake['displayName'] for lake in lh_cur]

    if cur_lakenames != dev_lakenames:
        print('Missing lakehouses')
        return 'Lakehouse missing'
    #loop the names
    for lh in cur_lakenames:
        tables_cur = notebookutils.lakehouse.listTables(lh)
        table_names_cur = [tb['name'] for tb in tables_cur]

        tables_dev = notebookutils.lakehouse.listTables(lh,dev_env)
        table_names_dev = [tb['name'] for tb in tables_dev]

        missing_tables = array_difference(table_names_dev,table_names_cur)

    #read 0 rows from dev table and then write it to the target table
        print('writing following tables: ',missing_tables, 'To ',lh)
        for missing_table in missing_tables:
        #get abfss_path of dev table
            abfss_path_dev = tables_dev[table_names_dev.index(missing_table)]['location']
            #print(abfss_path_dev)
            target_lakehouse_abfss_path = lh_cur[cur_lakenames.index(lh)]['properties'].get('abfsPath')
            #print(target_lakehouse_abfss_path)
            create_table_target(missing_table,abfss_path_dev, target_lakehouse_abfss_path)
       # get_create_table_statement(missing_table_name,abfss_path,target_lakehouse_abfss_path)

create_missing_tables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
