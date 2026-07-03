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

# # NB_FMD_PURVIEW_LINEAGE_TABLE_COLUMN
#
# ## Overview
# Standalone notebook that registers **column-level data lineage** for a single
# source → target step in Microsoft Purview via the Apache Atlas REST API.
#
# The notebook supports two ways to identify source and target datasets:
#
# | `source_type` / `target_type` value | How schema is resolved |
# |---|---|
# | `"table"` | Reads the Delta table schema from the attached Fabric Lakehouse using `Tables/{schema}/{table}` |
# | `"path"` | Reads the schema from an explicit ABFSS URI or lakehouse-relative path (Delta, Parquet, or CSV) |
#
# ### Purview Atlas entities created per call
# | Entity | Type | Qualified-name pattern |
# |---|---|---|
# | Source dataset | `azure_datalake_gen2_resource_set` | `onelake://{workspace_guid}/{source_lakehouse_name}/{schema}/{table}` |
# | Target dataset | `azure_datalake_gen2_resource_set` | `onelake://{workspace_guid}/{target_lakehouse_name}/{schema}/{table}` |
# | Process (with column mapping) | `Process` | `fmd://process/{layer}/{workspace_guid}/{schema}/{table}` |
#
# ## Parameters

# CELL ********************

from json import loads, dumps
import uuid
import re
import requests
import struct
from datetime import datetime, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ---------------------------------------------------------------------------
# Source dataset
# ---------------------------------------------------------------------------
# source_type: "table" — use source_schema + source_table (Delta table in attached lakehouse)
#              "path"  — use source_path + source_format (any readable path)
source_type = "table"          # "table" | "path"

source_lakehouse_name = ""     # e.g. "LH_BRONZE_LAYER" — used in Purview qualified name
source_schema = ""             # schema/folder name (table mode)
source_table = ""              # table name (table mode)
source_path = ""               # ABFSS URI or lakehouse-relative path (path mode)
source_format = "delta"        # "delta" | "parquet" | "csv"   (path mode)

# CSV-specific options (only used when source_format = "csv")
source_csv_header = True
source_csv_delimiter = ","
source_csv_infer_schema = True

# ---------------------------------------------------------------------------
# Target dataset
# ---------------------------------------------------------------------------
target_type = "table"          # "table" | "path"

target_lakehouse_name = ""     # e.g. "LH_SILVER_LAYER" — used in Purview qualified name
target_schema = ""             # schema/folder name (table mode)
target_table = ""              # table name (table mode)
target_path = ""               # ABFSS URI or lakehouse-relative path (path mode)
target_format = "delta"        # "delta" | "parquet" | "csv"   (path mode)

# CSV-specific options (only used when target_format = "csv")
target_csv_header = True
target_csv_delimiter = ","
target_csv_infer_schema = True

# ---------------------------------------------------------------------------
# Lineage metadata
# ---------------------------------------------------------------------------
# layer: descriptive label used in the Process qualified name and display name.
# Use one of the standard FMD values or supply a custom string.
layer = "landing_to_bronze"    # "landing_to_bronze" | "bronze_to_silver" | custom string

workspace_guid = ""            # Fabric workspace GUID (falls back to runtime context)
pipeline_run_guid = ""         # Optional — pipeline run GUID for audit trail

# ---------------------------------------------------------------------------
# Microsoft Purview settings
# ---------------------------------------------------------------------------
purview_account_name = ""      # e.g. "my-purview-account" (without .purview.azure.com)
purview_collection_name = ""   # Purview collection (leave blank for root)
purview_enabled = True         # Set False to perform a dry-run (no Purview POST)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve workspace GUID from runtime context when not supplied as a parameter
if not workspace_guid:
    workspace_guid = notebookutils.runtime.context.get('currentWorkspaceId') or '00000000-0000-0000-0000-000000000000'

if not pipeline_run_guid:
    pipeline_run_guid = str(uuid.uuid4())

starttime = datetime.now()

print(f"{'='*60}")
print(f"NB_FMD_PURVIEW_LINEAGE_TABLE_COLUMN")
print(f"{'='*60}")
print(f"Workspace  : {workspace_guid}")
print(f"Layer      : {layer}")
print(f"Source     : [{source_type}] {source_lakehouse_name} / {source_schema}.{source_table or source_path}")
print(f"Target     : [{target_type}] {target_lakehouse_name} / {target_schema}.{target_table or target_path}")
print(f"Purview    : {purview_account_name or '(not configured)'}")
print(f"Enabled    : {purview_enabled}")
print(f"{'='*60}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema Resolution

# CELL ********************

def _read_columns_from_table(schema: str, table: str) -> list:
    """
    Read column names from a Delta table via the attached Fabric Lakehouse.
    Path pattern: Tables/{schema}/{table}  or  Tables/{table}  when schema is empty.
    Returns a list of column-name strings; empty list on error.
    """
    load_path = f"Tables/{schema}/{table}" if schema else f"Tables/{table}"
    try:
        df = spark.read.format("delta").load(load_path)
        return [f.name for f in df.schema.fields]
    except Exception as e:
        print(f"  ⚠ Could not read table schema ({load_path}): {e}")
        return []


def _read_columns_from_path(path: str, fmt: str,
                             csv_header: bool, csv_delimiter: str,
                             csv_infer_schema: bool) -> list:
    """
    Read column names from an explicit path (ABFSS URI or lakehouse-relative).
    Supported formats: delta, parquet, csv.
    Returns a list of column-name strings; empty list on error.
    """
    if not path:
        print("  ⚠ source_path / target_path is empty — cannot resolve columns.")
        return []
    try:
        reader = spark.read.format(fmt)
        if fmt == "csv":
            reader = (
                reader
                .option("header", str(csv_header).lower())
                .option("delimiter", csv_delimiter)
                .option("inferSchema", str(csv_infer_schema).lower())
            )
        df = reader.load(path)
        return [f.name for f in df.schema.fields]
    except Exception as e:
        print(f"  ⚠ Could not read path schema ({path}, format={fmt}): {e}")
        return []


def resolve_columns(ds_type: str, schema: str, table: str,
                    path: str, fmt: str,
                    csv_header: bool, csv_delimiter: str,
                    csv_infer_schema: bool) -> list:
    """
    Dispatch to the correct schema reader based on ds_type ("table" | "path").
    """
    if ds_type == "table":
        return _read_columns_from_table(schema, table)
    if ds_type == "path":
        return _read_columns_from_path(path, fmt, csv_header, csv_delimiter, csv_infer_schema)
    print(f"  ⚠ Unknown dataset type '{ds_type}'. Expected 'table' or 'path'.")
    return []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Reading source schema...")
source_columns = resolve_columns(
    ds_type=source_type,
    schema=source_schema,
    table=source_table,
    path=source_path,
    fmt=source_format,
    csv_header=source_csv_header,
    csv_delimiter=source_csv_delimiter,
    csv_infer_schema=source_csv_infer_schema,
)
print(f"  Source columns ({len(source_columns)}): {source_columns[:10]}{'...' if len(source_columns) > 10 else ''}")

print("Reading target schema...")
target_columns = resolve_columns(
    ds_type=target_type,
    schema=target_schema,
    table=target_table,
    path=target_path,
    fmt=target_format,
    csv_header=target_csv_header,
    csv_delimiter=target_csv_delimiter,
    csv_infer_schema=target_csv_infer_schema,
)
print(f"  Target columns ({len(target_columns)}): {target_columns[:10]}{'...' if len(target_columns) > 10 else ''}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Column Mapping

# CELL ********************

def _build_column_mapping(source_cols: list, target_cols: list) -> str:
    """
    Build the Purview Atlas `columnMapping` attribute value.

    Returns a JSON-encoded string:
    [
      {
        "DatasetMapping": [
          {"Source": "<source_col>", "Sink": "<target_col>"},
          ...
        ]
      }
    ]

    Only columns present in both source and target (case-insensitive match) are mapped.
    Columns that exist only on one side are omitted.
    """
    target_lower = {c.lower(): c for c in target_cols}
    mappings = []
    for src_col in source_cols:
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

column_mapping_json = _build_column_mapping(source_columns, target_columns)
parsed = loads(column_mapping_json)
mapped_count = len(parsed[0]["DatasetMapping"]) if parsed else 0
print(f"Column mapping: {mapped_count} column(s) mapped between source and target.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Purview Atlas Payload

# CELL ********************

def _derive_qualified_name_parts(ds_type: str, lakehouse_name: str,
                                  schema: str, table: str, path: str) -> tuple:
    """
    Return (display_schema, display_table) used in Purview qualified names and display names.

    For table mode  : uses schema and table directly.
    For path mode   : derives a human-readable name from the last two path segments
                      so that the qualified name still reflects the physical location.
    """
    if ds_type == "table":
        return schema, table

    # Path mode — extract the last segment as a table substitute and the
    # second-to-last as a schema substitute when available.
    clean = path.rstrip("/")
    segments = [s for s in re.split(r"[/\\]", clean) if s]
    derived_table = segments[-1] if segments else path
    derived_schema = segments[-2] if len(segments) >= 2 else schema
    # Strip file extension if present
    derived_table = re.sub(r"\.[^.]+$", "", derived_table)
    return derived_schema, derived_table


def _build_atlas_payload(
    workspace_guid: str,
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
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
      - Process       (links the two with column-level mapping)

    Negative GUIDs (-1, -2, -3) are Atlas client-side temporary IDs within the
    same bulk request; Purview replaces them with permanent GUIDs on upsert.
    """
    src_prefix = f"{src_schema}/" if src_schema else ""
    tgt_prefix = f"{tgt_schema}/" if tgt_schema else ""

    source_qn = f"onelake://{workspace_guid}/{source_lakehouse_name}/{src_prefix}{src_table}"
    target_qn = f"onelake://{workspace_guid}/{target_lakehouse_name}/{tgt_prefix}{tgt_table}"
    process_qn = f"fmd://process/{layer}/{workspace_guid}/{tgt_prefix}{tgt_table}"

    common_attrs = {}
    if collection_name:
        common_attrs["collectionId"] = collection_name

    source_entity = {
        "typeName": "azure_datalake_gen2_resource_set",
        "guid": "-1",
        "attributes": {
            **common_attrs,
            "qualifiedName": source_qn,
            "name": f"{src_table} ({source_lakehouse_name})",
        }
    }

    target_entity = {
        "typeName": "azure_datalake_gen2_resource_set",
        "guid": "-2",
        "attributes": {
            **common_attrs,
            "qualifiedName": target_qn,
            "name": f"{tgt_table} ({target_lakehouse_name})",
        }
    }

    process_entity = {
        "typeName": "Process",
        "guid": "-3",
        "attributes": {
            **common_attrs,
            "qualifiedName": process_qn,
            "name": f"FMD {layer.replace('_', ' ').title()}: {tgt_schema}.{tgt_table}",
            "description": (
                f"FMD Framework automated processing step. "
                f"Pipeline run: {pipeline_run_guid}"
            ),
            "inputs":  [{"guid": "-1", "typeName": "azure_datalake_gen2_resource_set"}],
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

# Derive display names for source and target (handles both table and path modes)
src_display_schema, src_display_table = _derive_qualified_name_parts(
    source_type, source_lakehouse_name, source_schema, source_table, source_path
)
tgt_display_schema, tgt_display_table = _derive_qualified_name_parts(
    target_type, target_lakehouse_name, target_schema, target_table, target_path
)

atlas_payload = _build_atlas_payload(
    workspace_guid=workspace_guid,
    source_lakehouse_name=source_lakehouse_name,
    target_lakehouse_name=target_lakehouse_name,
    src_schema=src_display_schema,
    src_table=src_display_table,
    tgt_schema=tgt_display_schema,
    tgt_table=tgt_display_table,
    layer=layer,
    column_mapping_json=column_mapping_json,
    pipeline_run_guid=pipeline_run_guid,
    collection_name=purview_collection_name,
)

print("Atlas payload built:")
print(f"  Source QN : {atlas_payload['entities'][0]['attributes']['qualifiedName']}")
print(f"  Target QN : {atlas_payload['entities'][1]['attributes']['qualifiedName']}")
print(f"  Process QN: {atlas_payload['entities'][2]['attributes']['qualifiedName']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Microsoft Purview — Register Lineage

# CELL ********************

def _get_purview_token() -> str:
    """Obtain an AAD bearer token scoped to the Purview / Microsoft Purview resource."""
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

purview_status = "skipped"
purview_message = ""

if not purview_enabled:
    print("Purview lineage registration is disabled (purview_enabled=False). Dry-run only.")
elif not purview_account_name:
    print("⚠ purview_account_name is not set. Skipping Purview lineage registration.")
    purview_status = "skipped"
else:
    print(f"\n{'='*60}")
    print(f"Microsoft Purview — Registering Column-Level Lineage")
    print(f"Account   : {purview_account_name}.purview.azure.com")
    print(f"Collection: {purview_collection_name or '(root)'}")
    print(f"{'='*60}")

    try:
        token = _get_purview_token()
        headers = _purview_headers(token)
        url = _atlas_bulk_url(purview_account_name)

        response = requests.post(url, headers=headers, json=atlas_payload, timeout=60)

        if response.status_code in (200, 201):
            purview_status = "success"
            print(f"✓ Lineage registered successfully (HTTP {response.status_code})")
        else:
            purview_status = "failed"
            purview_message = f"HTTP {response.status_code}: {response.text[:400]}"
            print(f"⚠ Registration failed — {purview_message}")

    except Exception as exc:
        purview_status = "error"
        purview_message = str(exc)
        print(f"✗ Unexpected error during Purview registration: {exc}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TotalRuntime = str((datetime.now() - starttime))

exit_value = {
    "purview_status": purview_status,
    "purview_message": purview_message,
    "source": f"{source_lakehouse_name}/{src_display_schema}.{src_display_table}",
    "target": f"{target_lakehouse_name}/{tgt_display_schema}.{tgt_display_table}",
    "mapped_columns": mapped_count,
    "total_runtime": TotalRuntime,
}

print(f"\n{'='*60}")
print(f"Summary")
print(f"{'='*60}")
print(f"Status          : {purview_status}")
print(f"Mapped columns  : {mapped_count}")
print(f"Total runtime   : {TotalRuntime}")
print(f"{'='*60}")

notebookutils.notebook.exit(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
