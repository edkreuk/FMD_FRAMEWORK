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

from json import loads, dumps
import uuid
from datetime import datetime
NotebookExecutionId = str(uuid.uuid4())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

Path = ""
useRootDefaultLakehouse= True
PipelineRunGuid = ""
PipelineGuid = ""
TriggerGuid = ""
TriggerTime = ""
TriggerType = ""
key_vault =""

notebook_entities = ""


###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'
log_workspace_guid= ''
logwarehouse_guid = ''
logwarehouse_endpoint = ''
log_database = ''
log_client_id_secret_name = ""
log_client_secret_secret_name = ""

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
PipelineName = mssparkutils.runtime.context.get('currentNotebookName')
PipelineGuid = str(mssparkutils.runtime.context.get('currentNotebookId'))
WorkspaceGuid = mssparkutils.runtime.context.get('currentWorkspaceId')
PipelineParentRunGuid = mssparkutils.runtime.context.get('PipelineParentRunGuid')
PipelineRunGuid = str(uuid.uuid4())
TriggerGuid = format_guid(TriggerGuid)

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

if isinstance(Path, str):
    path_data = loads(Path)
elif isinstance(Path, List):
    path_data = Path
elif isinstance(Path, dict):
    path_data = [Path]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i, item in enumerate( path_data):
    item["notebook_path"] = item['path']
    data_source = item['params']['TargetSchema'].split('/')[0]
    schema_table = "".join(item['params']['TargetName'].split('_')[:2])
    item["notebook_activity_id"] = f"{data_source}_{schema_table}"
    item["params"]["PipelineGuid"] = PipelineGuid
    item["params"]["TriggerGuid"] = TriggerGuid
    item["params"]["TriggerType"] = TriggerType
    item["params"]["TriggerTime"] = TriggerTime
    item["params"]["WorkspaceGuid"] = WorkspaceGuid  
    item["params"]["PipelineParentRunGuid"] = PipelineParentRunGuid
    item["params"]["NotebookExecutionId"] = NotebookExecutionId
    item["params"]["useRootDefaultLakehouse"] = useRootDefaultLakehouse
    item["params"]["driver"] = driver
    item["params"]["log_workspace_guid"] = log_workspace_guid
    item["params"]["logwarehouse_endpoint"] = logwarehouse_endpoint
    item["params"]["logwarehouse_guid"] = logwarehouse_guid
    item["params"]["log_database"] = log_database
    item["params"]["log_client_id_secret_name"] = log_client_id_secret_name
    item["params"]["log_client_secret_secret_name"] = log_client_secret_secret_name



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Notebooks

# CELL ********************

notebooks = path_data
cmd_dags = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_concurrent_notebooks=50
for n in range((len(notebooks) // max_concurrent_notebooks) + 1):
    activities = [
        {
            "name": f"{nb['notebook_activity_id']}_{n}_{i}",  # activity name, must be unique
            "path": nb['notebook_path'],  # notebook path
            "timeoutPerCellInSeconds": 600,  # max timeout for each cell, default to 90 seconds
            "args": nb["params"],
            "retry": 2,  # max retry times, default to 0
            "retryIntervalInSeconds": 0
        }
        for i, nb in enumerate(notebooks[n * max_concurrent_notebooks:(n + 1) * max_concurrent_notebooks])
    ]
    cmd_dag = {
        "activities": activities,
        "timeoutInSeconds": 7200,  # max timeout for the entire pipeline, default to 12 hours now set to 2 hrs
        "concurrency": 0  # max number of notebooks to run concurrently, default to unlimited
    }
    cmd_dags.append(cmd_dag)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Execute Notebooks per 50, more is not supported by RunMultiple
results = {}
for cmd_dag in cmd_dags:
    try:
        results.update(notebookutils.mssparkutils.notebook.runMultiple(cmd_dag))
    except notebookutils.mssparkutils.handlers.notebookHandler.RunMultipleFailedException as e:
        results.update(e.result)
    except Exception as e:
        print(cmd_dag)
        raise(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert the data into a single result set
result_set = []
for table_name, table_data in results.items():
    result_set.append({
        "TableName": table_name,
        "exitVal": table_data["exitVal"],
        "exception": str(table_data["exception"])
    })
print(dumps(result_set, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fails = []
for result in result_set:
    if result.get('exception') != "None":
        fails.append(result)
if fails:
    raise ValueError(F"""Notebook(s): {[r['TableName'] for r in fails]} failed""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    exit_value = result_set
except Exception as e:
    print(e)
    exit_value= 'Error in Exit Value or No Notebooks to Process'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TotalRuntime = str((datetime.now() - starttime))
notebookutils.mssparkutils.notebook.exit(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
