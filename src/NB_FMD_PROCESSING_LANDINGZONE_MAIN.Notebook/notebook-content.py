# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # Parameters

# CELL ********************

import uuid
import requests

from datetime import datetime
from json import loads, dumps
from py4j.protocol import Py4JJavaError


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variable_settings=notebookutils.variableLibrary.getLibrary("VAR_FMD")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

###############################Target Parameters###############################
EntityId = ''
EntityLayer = "LandingZone"
DataSourceName = "NB"
TargetFilePath = ""
TargetFileName = ""
TargetLakehouseGuid = ""
WorkspaceGuid = ""
LastLoadValue = None
CustomNotebookName = "NB_FMD_CUSTOM_NOTEBOOK_TEMPLATE"
TriggerGuid = ""
TriggerTime = ""
TriggerType = ""
key_vault =variable_settings.key_vault_uri_name
###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'
connstring=variable_settings.fmd_fabric_db_connection
database=variable_settings.fmd_fabric_db_name
schema_enabled =variable_settings.lakehouse_schema_enabled
result_data=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def format_guid(input_str: str) -> str:
    """
    Formats the input string by adding hyphens at specific positions if they are missing.
    Parameters:
    - input_str (str): The input string to be formatted.
    Returns:
    - str: The formatted string.
    """
    if "-" not in input_str and len(input_str) == 32:
        formatted_str = '-'.join([input_str[:8], input_str[8:12], input_str[12:16], input_str[16:20], input_str[20:]])
        return formatted_str
    else:
        return input_str
def is_valid_guid(guid_str: str) -> bool:
    """
    Check if a string is a valid GUID.

    Parameters:
    - guid_str (str): The string to be checked.

    Returns:
    - bool: True if the string is a valid GUID, False otherwise.
    """
    try:
        uuid_obj = uuid.UUID(guid_str)
        # Check if the UUID is valid
        return str(uuid_obj) == guid_str
    except ValueError:
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## always use notebook guid instead of pipeline id
PipelineName = notebookutils.runtime.context.get('currentNotebookName')
PipelineGuid = str(notebookutils.runtime.context.get('currentNotebookId'))
PipelineParentRunGuid = notebookutils.runtime.context.get('PipelineParentRunGuid')
PipelineRunGuid = str(uuid.uuid4())
TriggerGuid = format_guid(TriggerGuid)
NotebookExecutionId = str(uuid.uuid4())

if not PipelineParentRunGuid:
    PipelineParentRunGuid='00000000-0000-0000-0000-000000000000'
if not PipelineGuid:
    PipelineGuid='00000000-0000-0000-0000-000000000000'
if not TriggerGuid or not is_valid_guid(TriggerGuid):
    TriggerGuid = '00000000-0000-0000-0000-000000000000'
if not TriggerTime:
    TriggerTime=''
if not TriggerType:
    TriggerType=''
if not WorkspaceGuid:
    WorkspaceGuid='00000000-0000-0000-0000-000000000000'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

starttime = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define Notebooks settings

# CELL ********************

notebook_params = {}
notebook_params["PipelineGuid"] = PipelineGuid
notebook_params["PipelineName"] = PipelineName
notebook_params["TriggerGuid"] = TriggerGuid
notebook_params["TriggerType"] = TriggerType
notebook_params["TriggerTime"] = TriggerTime
notebook_params["WorkspaceGuid"] = WorkspaceGuid  
notebook_params["PipelineParentRunGuid"] = PipelineParentRunGuid
notebook_params["PipelineRunGuid"] = PipelineRunGuid
notebook_params["NotebookExecutionId"] = NotebookExecutionId
notebook_params["driver"] = driver
notebook_params["connstring"] = connstring
notebook_params["database"] = database
notebook_params["EntityId"] = EntityId 
notebook_params["EntityLayer"] = EntityLayer 
notebook_params["DataSourceName"] = DataSourceName 
notebook_params["TargetFilePath"] = TargetFilePath 
notebook_params["TargetFileName"] = TargetFileName 
notebook_params["TargetLakehouseGuid"] = TargetLakehouseGuid 
notebook_params["WorkspaceGuid"] = WorkspaceGuid 
notebook_params["LastLoadValue"] = LastLoadValue 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Notebook

# CELL ********************

result = None
fail = None
try:
    result = notebookutils.notebook.run(CustomNotebookName, 900, notebook_params)
except Py4JJavaError as e:
    # Inspect the Java exception message
    if "NotebookExecutionException" in str(e):
        fail = e
except Exception as e:
    print(notebook_params)
    raise(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if fail:
    raise ValueError(F"""Notebook: {CustomNotebookName} failed with error: {e}""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TotalRuntime = str((datetime.now() - starttime))
notebookutils.mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
