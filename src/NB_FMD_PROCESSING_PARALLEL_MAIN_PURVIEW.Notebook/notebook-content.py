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

# ## Prerequisites
# Before running this notebook, ensure the following are in place:
# 
# ### Service Principal
# A dedicated **service principal (App Registration)** is required to authenticate against Microsoft Purview.
# 
# | Step | Action |
# |------|--------|
# | 1 | Create a service principal in Azure Active Directory (App Registration) |
# | 2 | Store the `tenant_id`, `client_id`, and `client_secret` in **Azure Key Vault** using the secret names configured in the parameters cell |
# 
# ### Microsoft Fabric — Workspace Access
# The service principal must be added as a **Member** to each data workspace that contains the source and target Lakehouses.
# 
# > Navigate to each Fabric workspace → **Manage access** → Add the service principal as **Member**.
# 
# ### Microsoft Purview — Role Assignment
# The service principal must have one of the following roles assigned in Microsoft Purview to register lineage and manage data assets:
# 
# - **Data Curator** — grants read/write access to data assets and lineage
# - **Data Source Admin** — grants full control over data source registration and scanning
# 
# > Navigate to **Microsoft Purview** → **Data Map** → **Collections** → select your collection → **Role assignments** → add the service principal to the desired role.
# 
# ## Parameters
# 
# - tenant_id="tenantid"
# - client_id="sp-fabric-purview-deployment-appid"
# - key_vault =default_settings.key_vault_uri_name  => make sure to set the keyvault setting in the variable Library
# - secret_name='sp-fabric-purview-deployment-secret'
# - PurviewAccount_name=> Purview account


# CELL ********************

from json import loads, dumps

import re
from datetime import datetime, timezone
from collections import defaultdict
from typing import List

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install pyapacheatlas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

Path = ""
useRootDefaultLakehouse= True

###############################Logging Parameters###############################
driver = '{ODBC Driver 18 for SQL Server}'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



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
        p.get("BrzLakehouseName"),
        p.get("BrzTableSchema01"),
        p.get("BrzTableName01")
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

for i, item in enumerate(path_data):
    item["notebook_path"] = item["path"]
    lakehouse = item["params"]["BrzLakehouseName"].split("/")[0]
    data_source = "".join(item["params"]["BrzTableSchema01"].split("_")[:2])
    schema_table = "".join(item["params"]["BrzTableName01"].split("_")[:2])
    item["notebook_activity_id"] = f"{lakehouse}_{data_source}_{schema_table}"

    # inject runtime params

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
    except (ValueError, AttributeError):
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
max_concurrent_notebooks = 20
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
            "retry": 1,
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
