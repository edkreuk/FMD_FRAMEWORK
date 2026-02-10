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

# # FMD Parallel Processing Main Notebook
# 
# ## Overview
# This notebook orchestrates parallel execution of data processing notebooks in the FMD framework. It handles batching, sequencing, and parallel execution of notebooks while managing dependencies and execution order.
# 
# ## Key Features
# - **Parallel Execution**: Executes multiple notebooks concurrently using `runMultiple` (max 50 per batch)
# - **Group Processing**: Groups related files by data source, target schema, and target table
# - **Sequential Ordering**: Processes files within the same group sequentially based on filename timestamps
# - **Batch Management**: Automatically creates execution batches while ensuring grouped items stay together
# - **Dependency Handling**: Maintains execution dependencies within file groups
# - **Auto-Discovery**: Automatically creates the custom DQ cleansing notebook if it doesn't exist
# 
# ## Parameters

# CELL ********************

from json import loads, dumps
import uuid
import re
from datetime import datetime, timezone
from collections import defaultdict
from typing import List

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

notebook_entities = ""


###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'


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

def extract_ts_from_name(name: str) -> datetime:
    """
    Extracts YYYYMMDDHHMM from names like 'Sales_Invoices_202601311402.parquet'
    """
    match = re.search(r'_(\d{12})(?=\.parquet$)', name)
    if not match:
        raise ValueError(f"Invalid filename timestamp: {name}")
    return datetime.strptime(match.group(1), "%Y%m%d%H%M")


def group_key(item):
    """
    Define what 'same files' means. Adjust as needed.
    Here: group by data source + target + partition path.
    """
    p = item["params"]
    return (
        p.get("DataSourceNamespace"),
        p.get("TargetSchema"),
        p.get("TargetName")
    )

def batched(lst, first_size, default_size):
    """Yield first batch with 'first_size', then others with 'default_size'."""
    if not lst:
        return
    yield lst[:first_size]
    pos = first_size
    while pos < len(lst):
        yield lst[pos:pos+default_size]
        pos += default_size


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## always use notebook guid instead of pipeline id
PipelineName = notebookutils.runtime.context.get('currentNotebookName')
PipelineGuid = str(notebookutils.runtime.context.get('currentNotebookId'))
WorkspaceGuid = notebookutils.runtime.context.get('currentWorkspaceId')
PipelineParentRunGuid = notebookutils.runtime.context.get('PipelineParentRunGuid')
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

# CELL ********************

nb_name = 'NB_FMD_CUSTOM_DQ_CLEANSING'
nb_exists = False

try:
    notebookutils.notebook.get(nb_name)
    nb_exists = True
except:
    nb_exists = False

print("=" * 50)
print(f"Notebook Name : {nb_name}")
print(f"Exists        : {nb_exists}")
print("=" * 50)

if not nb_exists:
    print(f"Creating       : Running...")
    import requests
    import sempy.fabric as fabric
    import json
    import base64
    
    access_token =  notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api')
    workspace_id = fabric.get_notebook_workspace_id()
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    cell_code = """
    # Implement custom cleansings function here

    #def <functienaam> (df, columns, args):
    #    print(args['<custom parameter name>']) # use of custom parameters
    #    for column in columns: # apply function foreach column
    #        df = df.<custom logic>
    #    return df #always return dataframe.
    """

    notebook_json = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": [
            {
                "cell_type": "code",
                "source": cell_code.strip().splitlines(keepends=True),
                "execution_count": None,
                "outputs": [],
                "metadata": {}
            }
        ],
        "metadata": {
            "language_info": {
                "name": "python"
            }
        }
    }

    notebook_str = json.dumps(notebook_json)
    notebook_bytes = notebook_str.encode('utf-8')
    notebook_base64 = base64.b64encode(notebook_bytes).decode('utf-8')

    payload = {
        "displayName": nb_name,
        "description": f"An automatic generated description for {nb_name}",
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": notebook_base64,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 201 or 202:
        print(f"Created        : Successful")
        print("=" * 50)
    else:
        print(f"Created                      : Error")
        print(f"Failed to create notebook    : {response.status_code}")
        print(response.text)
        print("=" * 50)

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

for i, item in enumerate(path_data):
    item["notebook_path"] = item["path"]
    data_source = item["params"]["TargetSchema"].split("/")[0]
    schema_table = "".join(item["params"]["TargetName"].split("_")[:2])
    item["notebook_activity_id"] = f"{data_source}_{schema_table}"

    # inject runtime params
    item["params"]["PipelineGuid"] = PipelineGuid
    item["params"]["PipelineName"] = PipelineName
    item["params"]["TriggerGuid"] = TriggerGuid
    item["params"]["TriggerType"] = TriggerType
    item["params"]["TriggerTime"] = TriggerTime
    item["params"]["WorkspaceGuid"] = WorkspaceGuid
    item["params"]["PipelineParentRunGuid"] = PipelineParentRunGuid
    item["params"]["PipelineRunGuid"] = PipelineRunGuid
    item["params"]["NotebookExecutionId"] = NotebookExecutionId
    item["params"]["useRootDefaultLakehouse"] = useRootDefaultLakehouse
    item["params"]["driver"] = driver


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Notebooks

# CELL ********************

#notebooks = path_data
cmd_dags = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# group by your definition of “same files”
groups = defaultdict(list)
for idx, it in enumerate(path_data):
    groups[group_key(it)].append((idx, it))

def safe_sort_key(pair):
    item = pair[1]
    name = item["params"].get("SourceFileName")

    if not name:
        return (datetime.max, "")   # no filename → put at end

    try:
        ts = extract_ts_from_name(name)
        return (ts, name)
    except:
        return (datetime.max, name) # malformed filename → also at end

largest_group_size = 1

for g, entries in list(groups.items()):
    sorted_entries = sorted(entries, key=safe_sort_key)

    for order_idx, (orig_idx, item) in enumerate(sorted_entries):
        item["is_grouped_job"] = True
        item["order_index"] = order_idx

    groups[g] = sorted_entries
    largest_group_size = max(largest_group_size, len(sorted_entries))

# Build a notebooks list that keeps groups intact
# Strategy: place grouped items contiguously, preserving their internal order.
# You can choose the order of groups; here we keep original appearance order.
seen_indices = set()
ordered_notebooks = []
for idx, it in enumerate(path_data):
    if idx in seen_indices:
        continue
    g = group_key(it)
    if g in groups:
        for orig_idx, mem in groups[g]:
            if orig_idx not in seen_indices:
                ordered_notebooks.append(mem)
                seen_indices.add(orig_idx)

# Add any remaining (non-grouped) items (if any)
for idx, it in enumerate(path_data):
    if idx not in seen_indices:
        ordered_notebooks.append(it)

# Batching with guarantee: no group split across batches
# Cap batches at 50 (runMultiple limit) while ensuring largest group fits in one batch
max_concurrent_notebooks = 50
if largest_group_size > max_concurrent_notebooks:
    print(f"WARNING: largest group has {largest_group_size} items, exceeds runMultiple limit of {max_concurrent_notebooks}")
first_batch_size = min(max_concurrent_notebooks, max(max_concurrent_notebooks, largest_group_size))

for n, batch in enumerate(batched(ordered_notebooks, first_batch_size, max_concurrent_notebooks)):
    activities = []
    # Track last activity name per group to wire dependsOn inside that group
    last_activity_name_by_group = {}

    for i, nb in enumerate(batch):
        activity_name = f"{nb['notebook_activity_id']}_{n}_{i}"
        activity = {
            "name": activity_name,
            "path": nb["notebook_path"],
            "timeoutPerCellInSeconds": 600,
            "args": nb["params"],
            "retry": 2,
            "retryIntervalInSeconds": 0
        }

        if nb.get("is_grouped_job"):
            g = group_key(nb)
            prev = last_activity_name_by_group.get(g)
            if prev is not None:
                # Chain to the previous activity within the same group
                activity["dependencies"] = [prev]
            last_activity_name_by_group[g] = activity_name

        activities.append(activity)

    cmd_dag = {
        "activities": activities,
        "timeoutInSeconds": 7200,
        "concurrency": len(activities)  # Allow concurrent execution; in-group dependencies enforced by dependsOn
    }
    cmd_dags.append(cmd_dag)
    print(f"Batch {n + 1}: {len(activities)} activities created")

print(f"\nTotal batches: {len(cmd_dags)} (runMultiple limit: 50 per batch)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute Notebooks in batches (runMultiple max: 50 notebooks per call)
results = {}
for batch_idx, cmd_dag in enumerate(cmd_dags):
    try:
        print(f"\nExecuting batch {batch_idx + 1}/{len(cmd_dags)} with {len(cmd_dag['activities'])} activities...")
        batch_results = notebookutils.mssparkutils.notebook.runMultiple(cmd_dag, {"displayDAGViaGraphviz": True})
        results.update(batch_results)
        print(f"✓ Batch {batch_idx + 1} completed successfully")
    except notebookutils.mssparkutils.handlers.notebookHandler.RunMultipleFailedException as e:
        print(f"⚠ Batch {batch_idx + 1} had errors, continuing with partial results")
        results.update(e.result)
    except Exception as e:
        print(f"\n✗ ERROR in batch {batch_idx + 1}:")
        print(f"  Activities: {len(cmd_dag['activities'])}")
        print(f"  Exception: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert the data into a single result set
result_set = []
for table_name, table_data in results.items():
    exception_str = str(table_data["exception"])
    result_set.append({
        "TableName": table_name,
        "exitVal": table_data["exitVal"],
        "exception": exception_str
    })

print(f"\n{'='*60}")
print(f"Execution Summary: {len(result_set)} activities completed")
print(f"{'='*60}")
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
    failed_names = [r['TableName'] for r in fails]
    print(f"\n✗ ERROR: {len(fails)} notebook execution(s) failed:")
    for f in fails:
        print(f"  - {f['TableName']}: {f['exception']}")
    raise ValueError(f"Failed notebooks: {failed_names}")

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
print(f"\n✓ Notebook execution completed in {TotalRuntime}")
notebookutils.notebook.exit(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
