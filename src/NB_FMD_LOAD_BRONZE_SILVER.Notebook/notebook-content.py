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

# PARAMETERS CELL ********************

# Set arguments
PrimaryKeys = "HashedPKColumn"
IsIncremental = False

SourceWorkspace= ""
SourceLakehouse =""
SourceLakehouseName =''
SourceSchema = ""
SourceName = ""
DataSourceNamespace =""
BronzeLayerEntityId =""
SilverLayerEntityId=""
TargetWorkspace= ""
TargetLakehouse =""
TargetLakehouseName =''
TargetSchema = ""
TargetName = ""
CleansingRules = []

###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'
schema_enabled = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Libraries

# CELL ********************

import re
import datetime
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from notebookutils import mssparkutils
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define Starttime

# CELL ********************

start_audit_time = datetime.datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Configuration

# CELL ********************

#Make sure you have enabled V-Order

spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set your loading paths

# CELL ********************

#Set SourceFile and target Location
if schema_enabled == True:
    source_changes_data_path = f"abfss://{SourceWorkspace}@onelake.dfs.fabric.microsoft.com/{SourceLakehouse}/Tables/{DataSourceNamespace}/{SourceSchema}_{SourceName}"
    print(source_changes_data_path)

    #Beware 
    target_data_path = f"abfss://{TargetWorkspace}@onelake.dfs.fabric.microsoft.com/{TargetLakehouse}/Tables/{DataSourceNamespace}/{TargetSchema}_{TargetName}"
    print(target_data_path)
elif schema_enabled  != True:
    source_changes_data_path = f"abfss://{SourceWorkspace}@onelake.dfs.fabric.microsoft.com/{SourceLakehouse}/Tables/{DataSourceNamespace}_{SourceSchema}_{SourceName}"
    print(source_changes_data_path)

    #Beware 
    target_data_path = f"abfss://{TargetWorkspace}@onelake.dfs.fabric.microsoft.com/{TargetLakehouse}/Tables/{DataSourceNamespace}_{TargetSchema}_{TargetName}"
    print(target_data_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read all incoming changes in Parquet format
dfDataChanged= spark.read\
                .format("delta") \
                .load(f"{source_changes_data_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(dfDataChanged)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Perform Cleansing

# CELL ********************

if cleansing_rules == "":
    cleansing_rules = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_FMD_DQ_CLEANSING

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

non_key_columns = [column for column in dfDataChanged.columns if column not in ('HashedPKColumn')]

#add a hashed cloumn to detect changes

dfDataChanged = dfDataChanged.withColumn("HashedNonKeyColumns", md5(concat_ws("||", *non_key_columns).cast(StringType())))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfDataChanged = dfDataChanged.withColumn('IsCurrent', lit(True).cast('boolean'))
dfDataChanged = dfDataChanged.withColumn('RecordStartDate', current_timestamp())
dfDataChanged = dfDataChanged.withColumn('RecordModifiedDate', current_timestamp())
dfDataChanged = dfDataChanged.withColumn('RecordEndDate', lit('9999-12-31').cast('timestamp'))
dfDataChanged = dfDataChanged.withColumn('IsDeleted', lit(False).cast('boolean'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Original if exists

# CELL ********************

#Check if Target exist, if exists read the original data if not create table and exit
if DeltaTable.isDeltaTable(spark, target_data_path):
    # Read original/current data
    dfDataOriginal = (spark
                        .read.format("delta")
                        .load(target_data_path)
                        )

else:
    # Use first load when no data exists yet and then exit 
    dfDataChanged.write.format("delta").mode("overwrite").save(target_data_path)
    TotalRuntime = str((datetime.datetime.now() - start_audit_time)) 

    # Your data
    result_data = {
        "CopyOutput":{
            "Total Runtime": TotalRuntime,
            "TargetSchema": TargetSchema,
            "TargetName" : TargetName,
            "EntityId" : SilverLayerEntityId,
            "StartTime" : start_audit_time,
            "EndTime" : end_audit_time

        }
        }


    mssparkutils.notebook.exit(result_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add columns for Merge SCD 2

# CELL ********************

#add a new column MergeKey based on the HashedPKColumn
dfDataChanged = dfDataChanged.withColumn('HashedPKColumn', dfDataChanged['HashedPKColumn'])
dfDataChanged = dfDataChanged.withColumn('Action', lit('U'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_insert_deletes = {f"changes.{column}" for column in dfDataOriginal.columns if column not in ('HashedPKColumn', 'Action')}
columns_original = {f"original.{column}" for column in dfDataOriginal.columns if column not in ('HashedPKColumn', 'Action')}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for changes

# CELL ********************

# Define columns to insert and delete
columns_to_insert_deletes = {f"changes.{column}" for column in dfDataOriginal.columns if column not in ('HashedPKColumn', 'Action')}

# Join DataFrames for inserts and deletes
# delete -> update table to close existing record
df_deletes = (
    dfDataOriginal.alias('original')
    .join(
        dfDataChanged.alias('changes'),
        (dfDataChanged.HashedPKColumn == dfDataOriginal.HashedPKColumn) &
        (dfDataOriginal.IsDeleted.cast('boolean') == lit(False).cast('boolean')),
        how='left'
    )
    .where("changes.HashedPKColumn is null")
    .where("original.IsCurrent == true")
    .select(
        "original.HashedPKColumn",
        "original.RecordStartDate",
        "original.HashedNonKeyColumns",
        lit('D').alias('Action'),
        *columns_to_insert_deletes
    )
    .withColumn('RecordEndDate', current_timestamp())
    .drop(col("changes.RecordStartDate"))
    .drop(col("changes.HashedNonKeyColumns"))
    .withColumn('IsCurrent', lit(False))
    .withColumn('IsDeleted', lit(True))
)

# Process updated records (new rows)
df_updates_new = (
    dfDataOriginal.alias('original')
    .join(
        dfDataChanged.alias('changes'),
        (dfDataChanged.HashedPKColumn == dfDataOriginal.HashedPKColumn) &
        (dfDataOriginal.IsDeleted.cast('boolean') == lit(False).cast('boolean')) &
        (dfDataOriginal.IsCurrent.cast('boolean') == lit(True).cast('boolean')),
        how='inner'
    )
    .where("changes.HashedNonKeyColumns <> original.HashedNonKeyColumns")
    .select(
        "changes.HashedPKColumn",
        lit('I').alias('Action'),
        *columns_to_insert_deletes
    )
    .withColumn('RecordEndDate', lit('9999-12-31').cast('timestamp'))
    .withColumn('IsCurrent', lit(True))
    .withColumn('IsDeleted', lit(False))
)

# Process updated records (old rows)
df_updates_old = (
    dfDataOriginal.alias('original')
    .join(
        dfDataChanged.alias('changes'),
        (dfDataChanged.HashedPKColumn == dfDataOriginal.HashedPKColumn) &
        (dfDataOriginal.IsDeleted.cast('boolean') == lit(False).cast('boolean')) &
        (dfDataOriginal.IsCurrent.cast('boolean') == lit(True).cast('boolean')),
        how='inner'
    )
    .where("changes.HashedNonKeyColumns <> original.HashedNonKeyColumns")
    .select(
        "changes.HashedPKColumn",
        "changes.RecordStartDate",
        lit('U').alias('Action'),
        *columns_original
    )
    .withColumn('RecordEndDate', expr("changes.RecordStartDate - interval 0.001 seconds"))
    .drop(col("changes.RecordStartDate"))
    .withColumn('IsCurrent', lit(False))
    .withColumn('IsDeleted', lit(False))
)

# Process inserted records
df_inserts = (
    dfDataChanged.alias('changes')
    .join(
        dfDataOriginal.alias('original'),
        (dfDataChanged.HashedPKColumn == dfDataOriginal.HashedPKColumn) &
        (dfDataOriginal.IsDeleted.cast('boolean') == lit(False).cast('boolean')),
        how='left'
    )
    .where("original.HashedPKColumn is null")
    .select(
        "changes.HashedPKColumn",
        lit('I').alias('Action'),
        *columns_to_insert_deletes
    )
    .withColumn('RecordEndDate', lit('9999-12-31').cast('timestamp'))
    .withColumn('IsCurrent', lit(True))
    .withColumn('IsDeleted', lit(False))
)

# Final merged DataFrame
dfDataChanged = (
    df_deletes
    .unionByName(df_updates_new)
    .unionByName(df_updates_old)
    .unionByName(df_inserts)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Merge table

# CELL ********************

columns_to_insert = {column: f"updates.{column}" for column in dfDataOriginal.columns if column not in ('IsCurrent', 'HashedNonKeyColumns', 'RecordStartDate', 'HashedPKColumn', 'RecordModifiedDate', 'RecordEndDate', 'IsDeleted', 'Action')}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, f'{target_data_path}')

merge = deltaTable.alias('original') \
    .merge(dfDataChanged.alias('updates'), 'original.HashedPKColumn = updates.HashedPKColumn and original.RecordStartDate = updates.RecordStartDate') \
    .whenMatchedUpdate(  # handle deletes
            condition="original.IsCurrent == True AND original.IsDeleted == False AND updates.Action = 'D'",
            set={
                "IsDeleted": lit(True),
                "RecordEndDate": col('updates.RecordEndDate')
            }) \
    .whenMatchedUpdate(  # handle updates
        condition="updates.HashedNonKeyColumns == original.HashedNonKeyColumns and original.IsCurrent = 1  ",
        set={
            "IsCurrent": lit(0),
            "RecordEndDate": col('updates.RecordStartDate')
        }) \
    .whenNotMatchedInsert(
        values={**columns_to_insert,
                "HashedPKColumn": col("updates.HashedPKColumn"),
                "HashedNonKeyColumns": col("updates.HashedNonKeyColumns"),
                "IsCurrent": lit(1),
                "RecordStartDate": current_timestamp(),
                "RecordModifiedDate": current_timestamp(),
                "RecordEndDate": lit('9999-12-31').cast('timestamp'),
                "IsDeleted": lit(0)})

# Execute the merge operation
merge.execute()

# Execute the merge operation
merge.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Exit notebook

# CELL ********************

TotalRuntime = str((datetime.datetime.now() - start_audit_time)) 
end_audit_time =  str(datetime.datetime.now())
start_audit_time =str(start_audit_time)
# Your data
result_data = {
    "CopyOutput":{
        "Total Runtime": TotalRuntime,
        "TargetSchema": TargetSchema,
        "TargetName" : TargetName,
        "EntityId" : SilverLayerEntityId,
        "StartTime" : start_audit_time,
        "EndTime" : end_audit_time

    }
    }


mssparkutils.notebook.exit(result_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
