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

# This file vacuums the delta tables, this is needed so they don't take unecessary space

# CELL ********************

from delta.tables import *
import concurrent.futures

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class roomba: 
    """
    A class to manage and optimize tables in lakehouses using Delta Lake's optimize and vacuum commands.
    The job is done multithreaded.

    Attributes:
        lakehouses (list): A list of lakehouse names to be managed.

    """
    def __init__(self,lakehouses):
        self.lakehouses = lakehouses
        self.start_roomba()

    def _roomba(self, lakehouseId,table_name):
        delta_table = DeltaTable.forPath(spark, f"/{lakehouseId}/Tables/{table_name}")
        try: 
        
            # Run the VACUUM command on the table
            delta_table.vacuum()
            #Optimize the table
            delta_table.optimize().executeCompaction()
            return (table_name,f"Success")
        except: 
            return (table_name,f"Can't vacuum table {table_name} it could be linked")

    def _get_workspace_id_and_lakehouseId(self,lh_name):
        lh = notebookutils.lakehouse.get(lh_name)
        lakehouseId = lh.id 
        return lakehouseId
    
    def _get_table_names(self,lh_name):
        tables = []
        for table in notebookutils.lakehouse.listTables(lh_name):
            tables.append(table.name)
        return tables

    def start_roomba(self):
        
        for lakehouse in self.lakehouses:
            lakehouseId = self._get_workspace_id_and_lakehouseId(lakehouse)
            tables = self._get_table_names(lakehouse)

            #Async job to loop all tables
            # TODO max_workers can be adjusted to have more/less concurrency! 
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(self._roomba,lakehouseId,table) for table in tables]

                #collect the data
                results = [future.result() for future in concurrent.futures.as_completed(futures)]

            for res in results:
                print(f"Table {lakehouse}.{res[0]} with status {res[1]}")
            #print(results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#clean spacific lakehouses

clean_houses = ['Raw','DP','Enr','Cur']
roomba(clean_houses)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Example to clean all lakehouses
#clean_all =[lh.get('displayName') for lh in notebookutils.lakehouse.list()]
#roomba(clean_all)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
