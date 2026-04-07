# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Custom Notebook Template
# This is a notebook template. Use this template to develop custom notebooks for extracting data.  
# An example for using a notebook for data extraction would be to extract data from an API.
# 
# ### Contents:
# - Imports
# - Notebook parameters
# - Custom Code
# - 
# 
# # WARNING
# 
# Make a copy of this notebook, every time you re deploy the framework this notebook will be overwritten

# CELL ********************

config_settings=notebookutils.variableLibrary.getLibrary("VAR_CONFIG_FMD")
default_settings=notebookutils.variableLibrary.getLibrary("VAR_FMD")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

import json
import pandas as pd
from pyspark.sql import DataFrame
from datetime import datetime, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Audit Start

# CELL ********************

start_audit_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

###############################Target Parameters###############################
EntityId = ""
EntityLayer = "LandingZone"
DataSourceName = ""
TargetFilePath = ""
TargetFileName = ""
TargetLakehouseGuid = ""
WorkspaceGuid = ""
LastLoadValue = ""
key_vault =default_settings.key_vault_uri_name
###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'
connstring=config_settings.fmd_fabric_db_connection
database=config_settings.fmd_fabric_db_name
schema_enabled =default_settings.lakehouse_schema_enabled
result_data=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Execution Logic

# CELL ********************

TriggerTime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
notebook_name=  notebookutils.runtime.context['currentNotebookName']

SP_AUDIT_NOTEBOOK = "[logging].[sp_AuditNotebook]"

audit_params = {
    "NotebookGuid": NotebookExecutionId,
    "NotebookName": notebook_name,
    "PipelineRunGuid": PipelineRunGuid,
    "PipelineParentRunGuid": PipelineParentRunGuid,
    "NotebookParameters": TargetFileName,
    "TriggerType": TriggerType,
    "TriggerGuid": TriggerGuid,
    "TriggerTime": TriggerTime,
    "WorkspaceGuid": WorkspaceGuid,
    "EntityId": EntityId,
    "EntityLayer": EntityLayer,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_FMD_UTILITY_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Configuration and Log Start 

# CELL ********************

execute_with_outputs(SP_AUDIT_NOTEBOOK, driver, connstring, database, **audit_params, LogData='{"Action":"Start"}', LogType="StartNotebookActivity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Set spark parquet settings for int96 and datetime data types
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

# ## Custom Code

# CELL ********************

## ====================== ##
## Start Custom code here ##
## ====================== ##
##  Should you implement  ##
##  incremental loading   ##
##  make sure to update   ##
##  the last load value   ##
## ====================== ##
##  Additionally, ensure  ##
##  that the output data  ##
##  is available as a     ##
##  spark dataframe with  ##
##  the following name:   ##
##  output_dataframe      ##
## ====================== ##

LoadValue = datetime.now()


sample_data = pd.DataFrame(
    data=[
        {"name": "John Doe",
         "age": 37
        },
        {"name": "Jane Doe",
         "age": 25
        },
        {"name": "Jimmy Doe",
         "age": 42
        }
    ])

sample_dataframe = spark.createDataFrame(sample_data)
output_dataframe = sample_dataframe

## ====================== ##
##  End Custom code here  ##
## ====================== ##


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write data to Onelake and exit

# CELL ********************

# Ensure the existence of an output_dataframe
if (not output_dataframe) or (type(output_dataframe) != DataFrame):
    raise Exception("No output_dataframe defined, or output_dataframe not a spark dataframe.")

# Write the output dataframe to Onelake
path = f"abfss://{WorkspaceGuid}@onelake.dfs.fabric.microsoft.com/{TargetLakehouseGuid}/Files/{TargetFilePath}/{TargetFileName}"
print(f"Target path: {path}")
output_dataframe.write.mode('overwrite').parquet(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gather the notebook run metrics

TotalRuntime = str((datetime.now() - start_audit_time)) 
end_audit_time =  str(datetime.now())
start_audit_time =str(start_audit_time)

result_data = {
    "Action" : "End",
    "LoadValue": str(LoadValue),
    "CopyOutput":{
        "Total Runtime": TotalRuntime,
        "DataSourceName": DataSourceName,
        "TargetFilePath" : TargetFilePath,
        "TargetFileName" : TargetFileName,
        "LandingzoneEntityId" : EntityId,
        "StartTime" : start_audit_time,
        "EndTime" : end_audit_time
    }
}

# Write the logging entry into the logging database
execute_with_outputs(SP_AUDIT_NOTEBOOK, driver, connstring, database, **audit_params, LogData=json.dumps(result_data), LogType="EndNotebookActivity")

# Exit the notebook
notebookutils.notebook.exit(result_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
