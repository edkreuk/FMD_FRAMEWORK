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

cleansing_rules = []

###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'
connstring=''
database=''
schema_enabled = ''
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
from datetime import datetime
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from notebookutils import mssparkutils
import uuid
import struct
import pyodbc

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
TriggerTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
notebook_name=  notebookutils.runtime.context['currentNotebookName']


UpsertPipelineLandingzoneEntity = (
    f"[execution].[sp_UpsertPipelineLandingzoneEntity] "
    f"@Filename = \"{SourceFileName}\", "
    f"@FilePath = \"{SourceFilePath}\", "
    f"@IsProcessed = \"True\", "
    f"@LandingzoneEntityId = \"{LandingzoneEntityId}\""
)

InsertPipelineBronzeLayerEntity = (
    f"[execution].[sp_UpsertPipelineBronzeLayerEntity] "
    f"@SchemaName = \"{TargetSchema}\", "
    f"@TableName = \"{TargetName}\", "
    f"@IsProcessed = \"False\", "
    f"@BronzeLayerEntityId = \"{BronzeLayerEntityId}\""
)

StartNotebookActivity = (
    f"[logging].[sp_AuditNotebook] "
    f"@NotebookGuid = \"{NotebookExecutionId}\", "
    f"@NotebookName = \"{notebook_name}\", "
    f"@PipelineRunGuid = \"{PipelineRunGuid}\", "
    f"@PipelineParentRunGuid = \"{PipelineParentRunGuid}\", "
    f"@NotebookParameters = \"{TargetName}\", "
    f"@TriggerType = \"{TriggerType}\", "
    f"@TriggerGuid = \"{TriggerGuid}\", "
    f"@TriggerTime = \"{TriggerTime}\", "
    f"@LogData = '{{\"Action\":\"Start\"}}', "
    f"@LogType = \"StartNotebookActivity\", "
    f"@WorkspaceGuid = \"{SourceWorkspace}\", "
    f"@EntityId = \"{BronzeLayerEntityId}\", "
    f"@EntityLayer = \"{EntityLayer}\""
)

EndNotebookActivity = (
    f"[logging].[sp_AuditNotebook] "
    f"@NotebookGuid = \"{NotebookExecutionId}\", "
    f"@NotebookName = \"{notebook_name}\", "
    f"@PipelineRunGuid = \"{PipelineRunGuid}\", "
    f"@PipelineParentRunGuid = \"{PipelineParentRunGuid}\", "
    f"@NotebookParameters = \"{TargetName}\", "
    f"@TriggerType = \"{TriggerType}\", "
    f"@TriggerGuid = \"{TriggerGuid}\", "
    f"@TriggerTime = \"{TriggerTime}\", "
    f"@LogType = \"EndNotebookActivity\", "
    f"@WorkspaceGuid = \"{SourceWorkspace}\", "
    f"@EntityId = \"{BronzeLayerEntityId}\", "
    f"@EntityLayer = \"{EntityLayer}\""
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Configuration

# CELL ********************

execute_with_logging(StartNotebookActivity, driver, connstring, database)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Make sure you have disabled V-Order, Bronze we want to load fast

spark.conf.set("sprk.sql.parquet.vorder.enabled", "false")

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

spark.conf.set('spark.microsoft.delta.optimize.fast.enabled', True)
spark.conf.set('spark.microsoft.delta.optimize.fileLevelTarget.enabled', True)
spark.conf.set('spark.databricks.delta.autoCompact.enabled', True)

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
    #Set SourceFile and target Location
    source_changes_data_path = f"abfss://{SourceWorkspace}@onelake.dfs.fabric.microsoft.com/{SourceLakehouse}/Files/{SourceFilePath}/{SourceFileName}"
    print(source_changes_data_path)

    #Beware 
    target_data_path = f"abfss://{TargetWorkspace}@onelake.dfs.fabric.microsoft.com/{TargetLakehouse}/Tables/{DataSourceNamespace}/{TargetSchema}_{TargetName}"
    print(target_data_path)
elif schema_enabled != True:
    #Set SourceFile and target Location
    source_changes_data_path = f"abfss://{SourceWorkspace}@onelake.dfs.fabric.microsoft.com/{SourceLakehouse}/Files/{SourceFilePath}/{SourceFileName}"
    print(source_changes_data_path)

    #Beware 
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

if dfDataChanged.select('HashedPKColumn').distinct().count() != dfDataChanged.select('HashedPKColumn').count():
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

%run NB_FMD_DQ_CLEANSING

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
dfDataChanged = dfDataChanged.withColumn("HashedNonKeyColumns", md5(concat_ws("||", *non_key_columns).cast(StringType())))

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

    execute_with_logging(UpsertPipelineLandingzoneEntity, driver, connstring, database)
    execute_with_logging(InsertPipelineBronzeLayerEntity, driver, connstring, database)
    execute_with_logging(EndNotebookActivity, driver, connstring, database, LogData=json.dumps(result_data))
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

execute_with_logging(UpsertPipelineLandingzoneEntity, driver, connstring, database)
execute_with_logging(InsertPipelineBronzeLayerEntity, driver, connstring, database)
execute_with_logging(EndNotebookActivity, driver, connstring, database, LogData=json.dumps(result_data))

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
