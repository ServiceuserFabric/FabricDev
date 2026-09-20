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

# # Enriched entity: Create Dimension Tables and DimensionSetEntry
# 
# ### Dependency:
# - DP.dp_dimensionvalues
# 
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>
# 
# 1. fetch the dimension list from FM_Utility
# 2. Create dimension tables for each dimension on the list
#     - Do transformations for each table
#     - Check for duplicates
#     - Writes each table dynamically. 
# 
# Currently following dimension are created: chain, branch, cost type and kitting

# MARKDOWN ********************

# ## Parameter

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create the dataframes

# CELL ********************

dimension_values = spark.read.table('DP.dp_dimensionvalues')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create dimensions that have been defined in bc_dimensions
dimensions = GlobalParameters.dimension_mapping


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#there are empty codes on Nav data so let's clean those out
dimension_values = dimension_values.filter(col('code').isNotNull() & (col('code') !=lit('')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#dims=[]
for dim in dimensions: 
    #Filter rows related to this dimension
    df = dimension_values.filter(expr(f"(dimensionCode ='{dim[0]}' AND source_system='bc' ) OR (dimensionCode = '{dim[1]}' and source_system = 'nav')"))

    dim_target = f"Enr.enr_dse_{dim[2]}" 

    #Create and rename columns
    df = df.select(
        
        #Schema of dimension tables
        'companyKey',
        col('code').alias(f"dse_{dim[2]}Code"),
        col('blocked'),
        col('dimensionCode'),
        col('dimensionId'),
        col('dimensionValueID'),
        col('name').alias(f"dse_{dim[2]}Name"),
        #Create a key between dimension and DSE.
        concat_ws('_',col('source_system'),
                    col('companyKey'),
                    when(expr("source_system = 'bc'"),col('dimensionValueID')).otherwise(col('code'))).alias("dimensionSetEntryKey"),
        
        concat_ws('_',col('source_system'),col('companyKey'),col('code')).alias(f"dse_{dim[2]}Key"),
        concat_ws('_',col('code'),col('name')).alias(f"dse_{dim[2]}Code_And_Name"),
        concat_ws(' - ',col('code'),col('name')).alias(f"dse_{dim[2]}Code_And_Name_Reporting"),
        col('source_system')
    )
    df = FM_Utility.add_nullhandling(df)

    print(f"\n ***************** \nWriting dimension {dim_target}")
    DataCheck.check_duplicates(df,f"dse_{dim[2]}Key")
    print(f"dim count dim{dim[2]}", df.count())
    print("*****************")

    display(df)

    df.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(dim_target)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
