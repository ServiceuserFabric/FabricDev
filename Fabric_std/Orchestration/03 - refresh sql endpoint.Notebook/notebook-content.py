# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
import time
from datetime import datetime
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

#Set UP


def pad_or_truncate_string(input_string, length, pad_char=' '):
    # Truncate if the string is longer than the specified length
    if len(input_string) > length:
        return input_string[:length]
    # Pad if the string is shorter than the specified length
    return input_string.ljust(length, pad_char)


#Instantiate the client
client = fabric.FabricRestClient()

# This is the SQL endpoint I want to sync with the lakehouse, this needs to be the GUI
sqlendpoint = notebookutils.lakehouse.getWithProperties(lakehouse_to_refresh).get('properties').get('sqlEndpointProperties').get('id')
#display(j_son)

# URI for the call
uri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint}"
# This is the action, we want to take
payload = {"commands":[{"$type":"MetadataRefreshCommand"}]}

# Call the REST API
response = client.post(uri,json= payload)
## You should add some error handling here

# return the response from json into an object we can get values from
data = json.loads(response.text)

# We just need this, we pass this to call to check the status
batchId = data["batchId"]

# the state of the sync i.e. inProgress
progressState = data["progressState"]

# URL so we can get the status of the sync
statusuri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint}/batches/{batchId}"

statusresponsedata = ""

while progressState == 'inProgress' :
    # For the demo, I have removed the 1 second sleep.
    time.sleep(5)

    # check to see if its sync'ed
    #statusresponse = client.get(statusuri)

    # turn response into object
    statusresponsedata = client.get(statusuri).json()

    # get the status of the check
    progressState = statusresponsedata["progressState"]
    # show the status
    display(f"Sync state: {progressState}")

# if its good, then create a temp results, with just the info we care about
if progressState == 'success':
    table_details = [
        {
          'tableName': table['tableName'],
         'warningMessages': table.get('warningMessages', []),
         'lastSuccessfulUpdate': table.get('lastSuccessfulUpdate', 'N/A'),
         'tableSyncState':  table['tableSyncState'],
         'sqlSyncState':  table['sqlSyncState']
        }
        for table in statusresponsedata['operationInformation'][0]['progressDetail']['tablesSyncStatus']
    ]

# if its good, then shows the tables
if progressState == 'success':
    # Print the extracted details
    print("Extracted Table Details:")
    for detail in table_details:
        print(f"Table: {pad_or_truncate_string(detail['tableName'],30)}   Last Update: {detail['lastSuccessfulUpdate']}  tableSyncState: {detail['tableSyncState']}   Warnings: {detail['warningMessages']}")

## if there is a problem, show all the errors
if progressState == 'failure':
    # display error if there is an error
    display(statusresponsedata)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
