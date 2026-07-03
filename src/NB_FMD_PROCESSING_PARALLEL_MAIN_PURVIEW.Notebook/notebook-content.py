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

# # NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW
#
# ## Overview
# This notebook extends `NB_FMD_PROCESSING_PARALLEL_MAIN` by adding **column-level data lineage**
# registration in Microsoft Purview after each parallel processing batch completes.
#
# For every entity processed (Landing Zone → Bronze or Bronze → Silver), the notebook:
# 1. Reads the source and target Delta table schemas from the Fabric Lakehouse.
# 2. Builds Apache Atlas entities (source table, target table, process) with a full column-to-column mapping.
# 3. Pushes the lineage payload to the Microsoft Purview Data Map via the Atlas REST API.
#
# Column-level lineage lets data teams trace exactly which source column feeds which target column
# through the FMD Medallion pipeline, visible in the Purview catalog lineage graph.
#
# ## Parameters

# CELL ********************

from json import loads, dumps
import uuid
import re
import requests
import struct
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
useRootDefaultLakehouse = True
PipelineRunGuid = ""
PipelineGuid = ""
TriggerGuid = ""
TriggerTime = ""
TriggerType = ""

notebook_entities = ""

# Microsoft Purview settings
purview_account_name = ""        # e.g. "my-purview-account" (without .purview.azure.com)
purview_collection_name = ""     # Purview collection to register assets in (leave blank for root)
purview_enabled = True           # Set to False to skip Purview lineage registration

###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def format_guid(input_str: str) -> str:
    if "-" not in input_str and len(input_str) == 32:
        return '-'.join([input_str[:8], input_str[8:12], input_str[12:16], input_str[16:20], input_str[20:]])
    return input_str

def is_valid_guid(guid_str: str) -> bool:
    try:
        uuid_obj = uuid.UUID(guid_str)
        return str(uuid_obj) == guid_str
    except ValueError:
        return False

def extract_ts_from_name(name: str) -> datetime:
    match = re.search(r'_(\d{12})(?=\.parquet$)', name)
    if not match:
        raise ValueError(f"Invalid filename timestamp: {name}")
    return datetime.strptime(match.group(1), "%Y%m%d%H%M")

def group_key(item):
    p = item["params"]
    return (
        p.get("DataSourceNamespace"),
        p.get("TargetSchema"),
        p.get("TargetName")
    )

def batched(lst, first_size, default_size):
    if not lst:
        return
    yield lst[:first_size]
    pos = first_size
    while pos < len(lst):
        yield lst[pos:pos + default_size]
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
    PipelineParentRunGuid = '00000000-0000-0000-0000-000000000000'
if not PipelineGuid:
    PipelineGuid = '00000000-0000-0000-0000-000000000000'
if not TriggerGuid or not is_valid_guid(TriggerGuid):
    TriggerGuid = '00000000-0000-0000-0000-000000000000'
if not TriggerTime:
    TriggerTime = ''
if not TriggerType:
    TriggerType = ''
if not WorkspaceGuid:
    WorkspaceGuid = '00000000-0000-0000-0000-000000000000'

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
except Exception as e:
    nb_exists = False
    print(f"Failed to check notebook existence for '{nb_name}': {e}")

print("=" * 50)
print(f"Notebook Name : {nb_name}")
print(f"Exists        : {nb_exists}")
print("=" * 50)

if not nb_exists:
    print(f"Creating       : Running...")
    import json
    import base64
    import sempy.fabric as fabric

    access_token = notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api')
    workspace_id = fabric.get_notebook_workspace_id()
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"

    headers = {
        "Authorization": f"******",
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

    if response.status_code in (201, 202):
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

# ## Define Notebook Settings

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

# ## Execute Notebooks in Parallel

# CELL ********************

cmd_dags = []

groups = defaultdict(list)
for idx, it in enumerate(path_data):
    groups[group_key(it)].append((idx, it))

def safe_sort_key(pair):
    item = pair[1]
    name = item["params"].get("SourceFileName")
    if not name:
        return (datetime.max, "")
    try:
        ts = extract_ts_from_name(name)
        return (ts, name)
    except (ValueError, AttributeError):
        return (datetime.max, name)


largest_group_size = 1

for g, entries in list(groups.items()):
    sorted_entries = sorted(entries, key=safe_sort_key)
    for order_idx, (orig_idx, item) in enumerate(sorted_entries):
        item["is_grouped_job"] = True
        item["order_index"] = order_idx
    groups[g] = sorted_entries
    largest_group_size = max(largest_group_size, len(sorted_entries))

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

for idx, it in enumerate(path_data):
    if idx not in seen_indices:
        ordered_notebooks.append(it)

max_concurrent_notebooks = 50
if largest_group_size > max_concurrent_notebooks:
    print(f"WARNING: largest group has {largest_group_size} items, exceeds runMultiple limit of {max_concurrent_notebooks}")
first_batch_size = min(max_concurrent_notebooks, max(max_concurrent_notebooks, largest_group_size))

for n, batch in enumerate(batched(ordered_notebooks, first_batch_size, max_concurrent_notebooks)):
    activities = []
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
                activity["dependencies"] = [prev]
            last_activity_name_by_group[g] = activity_name

        activities.append(activity)

    cmd_dag = {
        "activities": activities,
        "timeoutInSeconds": 7200,
        "concurrency": len(activities)
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

# MARKDOWN ********************

# ## Microsoft Purview — Column-Level Lineage Registration
#
# After all parallel notebooks complete successfully, the following cells read the source and
# target Delta table schemas and push column-level lineage to Microsoft Purview via the
# Apache Atlas REST API.
#
# ### How column-level lineage works in Purview
#
# Each processed entity results in three Atlas entities being upserted:
#
# | Atlas entity | Represents | Qualified name pattern |
# |---|---|---|
# | `azure_datalake_gen2_resource_set` | Source Delta table | `onelake://{workspace_guid}/{source_lakehouse}/{schema}/{table}` |
# | `azure_datalake_gen2_resource_set` | Target Delta table | `onelake://{workspace_guid}/{target_lakehouse}/{schema}/{table}` |
# | `Process` | FMD transformation step | `fmd://process/{layer}/{workspace_guid}/{schema}/{table}` |
#
# The `Process` entity carries a `columnMapping` attribute — a JSON string array where each
# entry maps a source column name to its corresponding target column name.

# CELL ********************

def _get_purview_token() -> str:
    """Obtain an AAD bearer token scoped to the Purview / Azure Purview resource."""
    return notebookutils.credentials.getToken('https://purview.azure.com')


def _purview_headers(token: str) -> dict:
    return {
        "Authorization": f"******",
        "Content-Type": "application/json"
    }


def _atlas_bulk_url(account_name: str) -> str:
    return f"https://{account_name}.purview.azure.com/datamap/api/atlas/v2/entity/bulk"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _detect_layer(notebook_path: str) -> str:
    """
    Derive the processing layer from the notebook path so that the correct
    source and target lakehouses can be referenced in the lineage payload.

    Returns one of: 'landing_to_bronze', 'bronze_to_silver', 'unknown'
    """
    path_upper = notebook_path.upper()
    if "LANDING_BRONZE" in path_upper or "LOAD_LANDING_BRONZE" in path_upper:
        return "landing_to_bronze"
    if "BRONZE_SILVER" in path_upper or "LOAD_BRONZE_SILVER" in path_upper:
        return "bronze_to_silver"
    return "unknown"


def _lakehouse_names(layer: str) -> tuple:
    """Return (source_lakehouse_name, target_lakehouse_name) for a given layer."""
    mapping = {
        "landing_to_bronze": ("LH_DATA_LANDINGZONE", "LH_BRONZE_LAYER"),
        "bronze_to_silver": ("LH_BRONZE_LAYER", "LH_SILVER_LAYER"),
    }
    return mapping.get(layer, ("UNKNOWN_SOURCE", "UNKNOWN_TARGET"))


def _read_delta_columns(lakehouse_name: str, schema: str, table: str) -> list:
    """
    Read the column names of a Delta table from the attached Fabric Lakehouse.
    Returns a list of column name strings.  Falls back to an empty list on error.
    """
    try:
        df = spark.read.format("delta").load(
            f"Tables/{schema}/{table}" if schema else f"Tables/{table}"
        )
        return [field.name for field in df.schema.fields]
    except Exception as e:
        print(f"  ⚠ Could not read schema for {lakehouse_name}.{schema}.{table}: {e}")
        return []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _build_column_mapping(source_columns: list, target_columns: list) -> str:
    """
    Build the Purview Atlas `columnMapping` attribute value.

    The value is a JSON-encoded string containing an array of DatasetMapping objects.
    Each DatasetMapping object maps one source column to the matching target column.
    Columns present in both source and target are mapped by name (case-insensitive match).
    Columns that exist only in one side are omitted.

    Purview column mapping format:
    [
      {
        "DatasetMapping": [
          {"Source": "<source_col>", "Sink": "<target_col>"},
          ...
        ]
      }
    ]
    """
    target_lower = {c.lower(): c for c in target_columns}
    mappings = []
    for src_col in source_columns:
        sink_col = target_lower.get(src_col.lower())
        if sink_col:
            mappings.append({"Source": src_col, "Sink": sink_col})

    if not mappings:
        return "[]"

    return dumps([{"DatasetMapping": mappings}])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _build_atlas_payload(
    workspace_guid: str,
    source_lakehouse: str,
    target_lakehouse: str,
    schema: str,
    table: str,
    layer: str,
    column_mapping_json: str,
    pipeline_run_guid: str,
    collection_name: str,
) -> dict:
    """
    Build the Apache Atlas bulk entity payload for one FMD processing step.

    Three entities are created / updated per call:
      - Source table  (azure_datalake_gen2_resource_set)
      - Target table  (azure_datalake_gen2_resource_set)
      - Process       (links the two tables with column-level mapping)

    The negative GUIDs (-1, -2, -3) are temporary Atlas client-side IDs used within
    the same bulk request.  Purview replaces them with permanent GUIDs on upsert.
    """
    schema_prefix = f"{schema}/" if schema else ""

    source_qn = f"onelake://{workspace_guid}/{source_lakehouse}/{schema_prefix}{table}"
    target_qn = f"onelake://{workspace_guid}/{target_lakehouse}/{schema_prefix}{table}"
    process_qn = f"fmd://process/{layer}/{workspace_guid}/{schema_prefix}{table}"

    common_attrs = {}
    if collection_name:
        common_attrs["collectionId"] = collection_name

    source_entity = {
        "typeName": "azure_datalake_gen2_resource_set",
        "guid": "-1",
        "attributes": {
            **common_attrs,
            "qualifiedName": source_qn,
            "name": f"{table} ({source_lakehouse})",
        }
    }

    target_entity = {
        "typeName": "azure_datalake_gen2_resource_set",
        "guid": "-2",
        "attributes": {
            **common_attrs,
            "qualifiedName": target_qn,
            "name": f"{table} ({target_lakehouse})",
        }
    }

    process_entity = {
        "typeName": "Process",
        "guid": "-3",
        "attributes": {
            **common_attrs,
            "qualifiedName": process_qn,
            "name": f"FMD {layer.replace('_', ' ').title()}: {schema}.{table}",
            "description": (
                f"FMD Framework automated processing step. "
                f"Pipeline run: {pipeline_run_guid}"
            ),
            "inputs": [{"guid": "-1", "typeName": "azure_datalake_gen2_resource_set"}],
            "outputs": [{"guid": "-2", "typeName": "azure_datalake_gen2_resource_set"}],
            "columnMapping": column_mapping_json,
        }
    }

    return {"entities": [source_entity, target_entity, process_entity]}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def register_purview_lineage(
    item: dict,
    workspace_guid: str,
    pipeline_run_guid: str,
    purview_account_name: str,
    purview_collection_name: str,
) -> dict:
    """
    Build and POST the column-level lineage payload for a single processed entity.

    Returns a dict with keys: entity, status, message.
    """
    params = item.get("params", {})
    target_schema = params.get("TargetSchema", "")
    target_name   = params.get("TargetName", "")
    notebook_path = item.get("notebook_path", "")

    layer = _detect_layer(notebook_path)
    source_lakehouse, target_lakehouse = _lakehouse_names(layer)

    print(f"\n  Entity   : {target_schema}.{target_name}")
    print(f"  Layer    : {layer}  ({source_lakehouse} → {target_lakehouse})")

    # Read schemas from Delta tables
    source_cols = _read_delta_columns(source_lakehouse, target_schema, target_name)
    target_cols  = _read_delta_columns(target_lakehouse, target_schema, target_name)

    print(f"  Columns  : source={len(source_cols)}, target={len(target_cols)}")

    column_mapping_json = _build_column_mapping(source_cols, target_cols)
    print(f"  Mapped   : {len(loads(column_mapping_json)[0]['DatasetMapping']) if loads(column_mapping_json) else 0} column(s)")

    payload = _build_atlas_payload(
        workspace_guid=workspace_guid,
        source_lakehouse=source_lakehouse,
        target_lakehouse=target_lakehouse,
        schema=target_schema,
        table=target_name,
        layer=layer,
        column_mapping_json=column_mapping_json,
        pipeline_run_guid=pipeline_run_guid,
        collection_name=purview_collection_name,
    )

    token   = _get_purview_token()
    headers = _purview_headers(token)
    url     = _atlas_bulk_url(purview_account_name)

    response = requests.post(url, headers=headers, json=payload, timeout=60)

    if response.status_code in (200, 201):
        print(f"  Purview  : ✓ Lineage registered (HTTP {response.status_code})")
        return {"entity": f"{target_schema}.{target_name}", "status": "success", "message": ""}
    else:
        msg = f"HTTP {response.status_code}: {response.text[:200]}"
        print(f"  Purview  : ⚠ Registration failed — {msg}")
        return {"entity": f"{target_schema}.{target_name}", "status": "failed", "message": msg}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

purview_results = []

if not purview_enabled:
    print("Purview lineage registration is disabled (purview_enabled=False). Skipping.")
elif not purview_account_name:
    print("⚠ purview_account_name is not set. Skipping Purview lineage registration.")
else:
    print(f"\n{'='*60}")
    print(f"Microsoft Purview — Column-Level Lineage Registration")
    print(f"Account  : {purview_account_name}.purview.azure.com")
    print(f"Collection: {purview_collection_name or '(root)'}")
    print(f"Entities  : {len(path_data)}")
    print(f"{'='*60}")

    for item in path_data:
        try:
            result = register_purview_lineage(
                item=item,
                workspace_guid=WorkspaceGuid,
                pipeline_run_guid=PipelineRunGuid,
                purview_account_name=purview_account_name,
                purview_collection_name=purview_collection_name,
            )
        except Exception as exc:
            entity_label = item.get("params", {}).get("TargetName", "unknown")
            print(f"  ✗ Unexpected error for {entity_label}: {exc}")
            result = {"entity": entity_label, "status": "error", "message": str(exc)}
        purview_results.append(result)

    succeeded = sum(1 for r in purview_results if r["status"] == "success")
    failed    = sum(1 for r in purview_results if r["status"] != "success")

    print(f"\n{'='*60}")
    print(f"Purview Lineage Summary: {succeeded} succeeded, {failed} failed")
    print(f"{'='*60}")
    if failed:
        for r in purview_results:
            if r["status"] != "success":
                print(f"  ✗ {r['entity']}: {r['message']}")

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
    exit_value = 'Error in Exit Value or No Notebooks to Process'

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
