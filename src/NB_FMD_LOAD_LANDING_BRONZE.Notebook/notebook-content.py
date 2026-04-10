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
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # FMD Load Landing Zone to Bronze Notebook
# 
# ## Overview
# This notebook handles the data loading process from the Landing Zone to the Bronze layer in the FMD framework. It processes source files, applies data quality checks, performs cleansing, and loads data into Bronze Delta tables.
# 
# ## Key Features
# - **Source File Validation**: Checks if source files exist before processing
# - **Data Quality Checks**: Validates primary keys and detects duplicates
# - **Cleansing Rules**: Applies configurable cleansing rules from the framework database
# - **Change Detection**: Uses hash columns to detect changes in data
# - **Incremental Loading**: Supports both full and incremental load patterns
# - **Audit Logging**: Tracks execution details in the framework database
# - **Delta Lake Integration**: Writes data to Delta tables with optimization settings
# 
# ## Process Flow
# 1. Load libraries and configuration settings
# 2. Set up audit logging and database connections
# 3. Read source file from Landing Zone (Parquet/CSV)
# 4. Perform data quality checks (PK validation, duplicate detection)
# 5. Apply cleansing rules from framework configuration
# 6. Add hash columns for change tracking
# 7. Execute incremental or full load to Bronze Delta table
# 8. Update processing status and complete audit logging


# CELL ********************

config_settings=notebookutils.variableLibrary.getLibrary("VAR_CONFIG_FMD")
default_settings=notebookutils.variableLibrary.getLibrary("VAR_FMD")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Set arguments
PrimaryKeys = ""
SourceFileType='parquet'
IsIncremental = False

SourceWorkspace= ''
SourceLakehouse = ''
SourceLakehouseName = ''
SourceFilePath = ''
SourceFileName = ''
DataSourceNamespace = ''

TargetWorkspace = ''
TargetLakehouse = ''
TargetLakehouseName = ''
TargetSchema = ''
TargetName = ''

LandingzoneEntityId =""
BronzeLayerEntityId =""
# # CSV
CompressionType = 'infer'
ColumnDelimiter = ','
RowDelimiter = '\n'
EscapeCharacter = '"'
Encoding = 'UTF-8'
first_row_is_header = True
infer_schema = True
key_vault =default_settings.key_vault_uri_name
cleansing_rules = []
dq_rules = []

###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'
connstring=config_settings.fmd_fabric_db_connection
database=config_settings.fmd_fabric_db_name
schema_enabled =default_settings.lakehouse_schema_enabled
EntityLayer='Bronze'
result_data=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Libraries

# CELL ********************

import re
from datetime import datetime, timezone
import json
from delta.tables import *
from pyspark.sql.functions import sha2, concat_ws, current_timestamp
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define Starttime

# CELL ********************

start_audit_time = datetime.now()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token =  notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execution Logic

# CELL ********************

%run NB_FMD_UTILITY_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define Stored Procedures for Logging

# CELL ********************

# Ensure TriggerTime is formatted correctly
TriggerTime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
notebook_name=  notebookutils.runtime.context['currentNotebookName']


# Stored procedure names
SP_UPSERT_LDZ_ENTITY = "[execution].[sp_UpsertPipelineLandingzoneEntity]"
SP_UPSERT_BRONZE_ENTITY = "[execution].[sp_UpsertPipelineBronzeLayerEntity]"
SP_AUDIT_NOTEBOOK = "[logging].[sp_AuditNotebook]"
SP_GET_CLEANSING_RULE = "[execution].[sp_GetBronzeCleansingRule]"
SP_GET_DQ_RULE = "[execution].[sp_GetBronzeDQRule]"

# Common audit parameters
audit_params = {
    "NotebookGuid": NotebookExecutionId,
    "NotebookName": notebook_name,
    "PipelineRunGuid": PipelineRunGuid,
    "PipelineParentRunGuid": PipelineParentRunGuid,
    "NotebookParameters": TargetName,
    "TriggerType": TriggerType,
    "TriggerGuid": TriggerGuid,
    "TriggerTime": TriggerTime,
    "WorkspaceGuid": SourceWorkspace,
    "EntityId": BronzeLayerEntityId,
    "EntityLayer": EntityLayer,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Configuration

# CELL ********************

execute_with_outputs(SP_AUDIT_NOTEBOOK, driver, connstring, database, **audit_params, LogData='{"Action":"Start"}', LogType="StartNotebookActivity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Make sure you have disabled V-Order, Bronze we want to load fast
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

spark.conf.set('spark.microsoft.delta.optimize.fast.enabled', True)
spark.conf.set('spark.microsoft.delta.optimize.fileLevelTarget.enabled', True)
spark.conf.set('spark.databricks.delta.autoCompact.enabled', True)

spark.conf.set("spark.fabric.resourceProfile", "writeHeavy")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set your loading paths

# CELL ********************

#Set SourceFile and target Location
source_changes_data_path = f"abfss://{SourceWorkspace}@onelake.dfs.fabric.microsoft.com/{SourceLakehouse}/Files/{SourceFilePath}/{SourceFileName}"
print(source_changes_data_path)

if str(schema_enabled).lower() == "true":
    target_data_path = f"abfss://{TargetWorkspace}@onelake.dfs.fabric.microsoft.com/{TargetLakehouse}/Tables/{DataSourceNamespace}/{TargetSchema}_{TargetName}"
else:
    target_data_path = f"abfss://{TargetWorkspace}@onelake.dfs.fabric.microsoft.com/{TargetLakehouse}/Tables/{DataSourceNamespace}_{TargetSchema}_{TargetName}"
print(target_data_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load new from Data Landingzone

# CELL ********************

if not notebookutils.fs.exists(source_changes_data_path):
    print("❌ Source file not found. Exiting Notebook")
    execute_with_outputs(SP_UPSERT_LDZ_ENTITY, driver, connstring, database, Filename=SourceFileName, FilePath=SourceFilePath, IsProcessed="True", LandingzoneEntityId=LandingzoneEntityId)
    TotalRuntime = str((datetime.now() - start_audit_time))
    end_audit_time =  str(datetime.now())
    start_audit_time =str(start_audit_time)
    result_data = {
    "Action" : "End", "CopyOutput":{
        "Total Runtime": TotalRuntime,
        "TargetSchema": TargetSchema,
        "TargetName" : TargetName,
        "SourceFilePath" : SourceFilePath,
        "SourceFileName" : 'FILE NOT FOUND',
        "LandingzoneEntityId" : LandingzoneEntityId,
        "EntityId" : BronzeLayerEntityId,
        "StartTime" : start_audit_time,
        "EndTime" : end_audit_time

    }
    }
    execute_with_outputs(SP_AUDIT_NOTEBOOK, driver, connstring, database, **audit_params, LogData=json.dumps(result_data), LogType="EndNotebookActivity")
    
    notebookutils.notebook.exit(result_data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if SourceFileType=='csv':
    dfDataChanged = (
        spark.read
            .option("header", True)            # first row has column names
            .option("inferSchema", True)       # ask Spark to infer types
            .option("samplingRatio", 0.1)      # sample 10% of rows; omit to scan fully
            .csv(f"{source_changes_data_path}")
    )
elif SourceFileType=='xlsx':
    # Basic read: entire first sheet, header row present, types inferred
    import pandas as pd
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    pdf = pd.read_excel(f"{source_changes_data_path}", engine="openpyxl")
    dfDataChanged = spark.createDataFrame(pdf)

elif SourceFileType=='xls':
    # Basic read: entire first sheet, header row present, types inferred
    import pandas as pd
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    pdf = pd.read_excel(f"{source_changes_data_path}", engine="xlrd")
    dfDataChanged = spark.createDataFrame(pdf)

else:
    #Read all incoming changes in Parquet format
    dfDataChanged= spark.read\
                    .format(SourceFileType) \
                    .option("header","true") \
                    .load(f"{source_changes_data_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace spaces with underscores in column names
new_columns = [col.replace(' ', '') for col in dfDataChanged.columns]

# Rename the columns
dfDataChanged = dfDataChanged.toDF(*new_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DQ Checks

# CELL ********************

#split PKcolumns string on , ; or :
PrimaryKeys = str(PrimaryKeys)

PrimaryKeys = re.split('[, ; :]', PrimaryKeys)
#remove potential whitespaces around Pk columns 
PrimaryKeys = [column.strip() for column in PrimaryKeys if column != ""]

key_columns = PrimaryKeys
print(f": {', '.join(key_columns)}")
# Check if all PK's exist in source
for pk_column in key_columns:
    if pk_column not in dfDataChanged.columns:
        raise ValueError(f"PK: {pk_column} doesn't exist in the source.")
        # Define all the Non-Key columns => HashExcludeColumns

read_key_columns = [column for column in dfDataChanged.columns if column in key_columns]

# Add a column with the calculated hash, easier in later stage of with multiple PK
dfDataChanged = (dfDataChanged
                .withColumn("HashedPKColumn", sha2(concat_ws("||", *read_key_columns), 256)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for Duplicates

# CELL ********************

dup_count = dfDataChanged.groupBy('HashedPKColumn').count().where('count > 1').limit(1).collect()
if dup_count:
    raise ValueError(f'Source file contains duplicated rows for PK: {", ".join(key_columns)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Perform Cleansing

# CELL ********************

if cleansing_rules == "":
    cleansing_rules = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CleansingRules=execute_with_outputs(SP_GET_CLEANSING_RULE, driver, connstring, database, BronzeLayerEntityId=BronzeLayerEntityId)
rules_str = None
# Extract the string
rules_str = CleansingRules["result_sets"][0][0]["CleansingRules"]
if rules_str != None :
# Convert JSON text → Python dict/list
    cleansing_rules = json.loads(rules_str)

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

dfDataChanged=handle_cleansing_functions(dfDataChanged,cleansing_rules)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add Hash

# CELL ********************

non_key_columns = [column for column in dfDataChanged.columns if column not in key_columns]

#add a hashed cloumn to detect changes
dfDataChanged = dfDataChanged.withColumn("HashedNonKeyColumns", sha2(concat_ws("||", *non_key_columns).cast(StringType()), 256))

#Add RecordLoadDate to see when the record arrived
dfDataChanged = dfDataChanged.withColumn('RecordLoadDate', current_timestamp())


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
    TotalRuntime = str((datetime.now() - start_audit_time)) 
    TotalRuntime = str((datetime.now() - start_audit_time)) 
    end_audit_time =  str(datetime.now())
    start_audit_time =str(start_audit_time)
    # Your data
    result_data = {
        "Action" : "End", "CopyOutput":{
            "Total Runtime": TotalRuntime,
            "TargetSchema": TargetSchema,
            "TargetName" : TargetName,
            "SourceFilePath" : SourceFilePath,
            "SourceFileName" : SourceFileName,
            "LandingzoneEntityId" : LandingzoneEntityId,
            "EntityId" : BronzeLayerEntityId,
            "StartTime" : start_audit_time,
            "EndTime" : end_audit_time

        }
        }

    execute_with_outputs(SP_UPSERT_LDZ_ENTITY, driver, connstring, database, Filename=SourceFileName, FilePath=SourceFilePath, IsProcessed="True", LandingzoneEntityId=LandingzoneEntityId)
    execute_with_outputs(SP_UPSERT_BRONZE_ENTITY, driver, connstring, database, SchemaName=TargetSchema, TableName=TargetName, IsProcessed="False", BronzeLayerEntityId=BronzeLayerEntityId)
    execute_with_outputs(SP_AUDIT_NOTEBOOK, driver, connstring, database, **audit_params, LogData=json.dumps(result_data), LogType="EndNotebookActivity")
    notebookutils.notebook.exit(result_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Merge table

# CELL ********************

#merge table 
deltaTable = DeltaTable.forPath(spark, f'{target_data_path}')
if IsIncremental in [False, 'false', 'False']:
    print(' - Incremental Loading is not enabled, deletes are allowed')
    merge = deltaTable.alias('original') \
        .merge(dfDataChanged.alias('updates'), 'original.HashedPKColumn == updates.HashedPKColumn') \
        .whenNotMatchedInsertAll() \
        .whenMatchedUpdateAll('original.HashedNonKeyColumns != updates.HashedNonKeyColumns') \
        .whenNotMatchedBySourceDelete() \
        .execute()
elif IsIncremental not in [False, 'false', 'False']:
    print(' - Incremental Loading is enabled, deletes are not allowed')
    merge = deltaTable.alias('original') \
        .merge(dfDataChanged.alias('updates'), 'original.HashedPKColumn == updates.HashedPKColumn') \
        .whenNotMatchedInsertAll() \
        .whenMatchedUpdateAll('original.HashedNonKeyColumns != updates.HashedNonKeyColumns') \
        .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define Results

# CELL ********************

TotalRuntime = str((datetime.now() - start_audit_time)) 
end_audit_time =  str(datetime.now())
start_audit_time =str(start_audit_time)
# Your data
result_data = {
    "Action" : "End", "CopyOutput":{
        "Total Runtime": TotalRuntime,
        "TargetSchema": TargetSchema,
        "TargetName" : TargetName,
        "SourceFilePath" : SourceFilePath,
        "SourceFileName" : SourceFileName,
        "LandingzoneEntityId" : LandingzoneEntityId,
        "EntityId" : BronzeLayerEntityId,
        "StartTime" : start_audit_time,
        "EndTime" : end_audit_time

    }
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Logging and update queue

# CELL ********************

execute_with_outputs(SP_UPSERT_LDZ_ENTITY, driver, connstring, database, Filename=SourceFileName, FilePath=SourceFilePath, IsProcessed="True", LandingzoneEntityId=LandingzoneEntityId)
execute_with_outputs(SP_UPSERT_BRONZE_ENTITY, driver, connstring, database, SchemaName=TargetSchema, TableName=TargetName, IsProcessed="False", BronzeLayerEntityId=BronzeLayerEntityId)
execute_with_outputs(SP_AUDIT_NOTEBOOK, driver, connstring, database, **audit_params, LogData=json.dumps(result_data), LogType="EndNotebookActivity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Notebook exit

# CELL ********************

notebookutils.notebook.exit(result_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
