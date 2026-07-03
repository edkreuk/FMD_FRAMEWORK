# Microsoft Purview Integration with the FMD Framework

## Overview

[Microsoft Purview](https://learn.microsoft.com/en-us/purview/purview) is a unified data governance solution that provides data discovery, classification, lineage, and compliance capabilities across your entire data estate. When used alongside the **Fabric Metadata-Driven Framework (FMD)**, Purview gives data teams a complete picture of where data comes from, how it flows through the Medallion layers, and how it is classified — all from a single portal.

The FMD Framework provides a dedicated notebook — **`NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW`** — that extends the standard parallel processing notebook to automatically push **column-level lineage** to Microsoft Purview after every processing batch. This gives you a precise, column-to-column trace from Landing Zone through Bronze and Silver in the Purview lineage graph.

This page describes:
1. How `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW` works and how to configure it.
2. The column-level lineage data model used (Apache Atlas entities).
3. How to connect Microsoft Fabric to Microsoft Purview and view the resulting lineage.
4. How to scan your FMD lakehouses and apply sensitivity labels.

---

## NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW

### What the notebook does

`NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW` is a drop-in replacement for `NB_FMD_PROCESSING_PARALLEL_MAIN`. It performs the same parallel orchestration of Bronze and Silver processing notebooks and then, once all batches succeed, registers column-level lineage in Microsoft Purview for every entity that was processed.

**Processing flow:**

```
Pipeline trigger
    │
    ▼
NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW
    │
    ├─► [Batch 1..N] runMultiple → NB_FMD_LOAD_LANDING_BRONZE (parallel)
    │                            → NB_FMD_LOAD_BRONZE_SILVER   (parallel)
    │
    └─► [After all batches] For each entity:
            1. Read source Delta table schema  (Spark)
            2. Read target Delta table schema  (Spark)
            3. Build column mapping JSON
            4. POST Atlas entities to Purview  (REST API)
```

### Parameters

The notebook accepts all parameters from `NB_FMD_PROCESSING_PARALLEL_MAIN` plus three Purview-specific parameters:

| Parameter | Type | Description |
|---|---|---|
| `Path` | String (JSON) | Entity list — same format as `NB_FMD_PROCESSING_PARALLEL_MAIN` |
| `useRootDefaultLakehouse` | Boolean | Use the root default lakehouse for Delta reads |
| `PipelineRunGuid` | String | Pipeline run GUID (injected by the calling pipeline) |
| `PipelineGuid` | String | Parent pipeline GUID |
| `TriggerGuid` | String | Trigger GUID |
| `TriggerTime` | String | Trigger timestamp |
| `TriggerType` | String | Trigger type (e.g. `Schedule`) |
| `driver` | String | ODBC driver (default: `{ODBC Driver 18 for SQL Server}`) |
| **`purview_account_name`** | String | Purview account name **without** `.purview.azure.com` (e.g. `my-purview`) |
| **`purview_collection_name`** | String | Purview collection to register assets in (leave blank for root collection) |
| **`purview_enabled`** | Boolean | Set to `False` to run without pushing lineage (default: `True`) |

### How to replace the standard notebook

To switch from `NB_FMD_PROCESSING_PARALLEL_MAIN` to `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW`:

1. Open `PL_FMD_LOAD_BRONZE` (or `PL_FMD_LOAD_SILVER`, `PL_FMD_LOAD_ALL`) in your Code workspace.
2. Find the **Notebook activity** that calls `NB_FMD_PROCESSING_PARALLEL_MAIN`.
3. Change the notebook reference to `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW`.
4. Add the three new parameters (`purview_account_name`, `purview_collection_name`, `purview_enabled`) to the activity's parameter settings.
5. Save and publish the pipeline.

---

## Column-Level Lineage Data Model

### Apache Atlas entity types

Purview uses the [Apache Atlas](https://atlas.apache.org/) data model internally. For each FMD entity that is processed, the notebook upserts **three Atlas entities** via the Purview Data Map REST API:

| Atlas entity | `typeName` | Represents |
|---|---|---|
| Source table | `azure_datalake_gen2_resource_set` | Input Delta table (e.g. Bronze layer) |
| Target table | `azure_datalake_gen2_resource_set` | Output Delta table (e.g. Silver layer) |
| Process | `Process` | The FMD transformation step that links them |

> `azure_datalake_gen2_resource_set` is used because Microsoft Fabric Lakehouses are backed by OneLake, which presents as Azure Data Lake Storage Gen2 from the perspective of the Purview scanner.

### Qualified name patterns

Every Atlas entity is identified by a `qualifiedName`. The notebook constructs qualified names as follows:

| Entity | Qualified name pattern |
|---|---|
| Source Delta table | `onelake://{workspace_guid}/{source_lakehouse}/{schema}/{table}` |
| Target Delta table | `onelake://{workspace_guid}/{target_lakehouse}/{schema}/{table}` |
| Process | `fmd://process/{layer}/{workspace_guid}/{schema}/{table}` |

Where `{layer}` is one of:
- `landing_to_bronze` — for `NB_FMD_LOAD_LANDING_BRONZE` notebooks
- `bronze_to_silver` — for `NB_FMD_LOAD_BRONZE_SILVER` notebooks

### The `columnMapping` attribute

The `Process` entity carries a `columnMapping` attribute that encodes the column-to-column mapping as a JSON string. Purview reads this attribute to render the column lineage graph in the catalog.

**Format:**

```json
[
  {
    "DatasetMapping": [
      { "Source": "CustomerID",   "Sink": "CustomerID"   },
      { "Source": "CustomerName", "Sink": "CustomerName" },
      { "Source": "EmailAddress", "Sink": "EmailAddress" },
      { "Source": "ModifiedDate", "Sink": "ModifiedDate" }
    ]
  }
]
```

**How the mapping is built:**

1. The notebook reads the source Delta table schema with `spark.read.format("delta").load(...)`.
2. It reads the target Delta table schema the same way.
3. Columns that exist in **both** source and target (matched case-insensitively by name) are included.
4. Columns that exist only in the source (e.g. raw ingestion metadata) or only in the target (e.g. SCD Type 2 audit columns) are omitted from the mapping.

### Example Atlas payload

Below is a representative payload that the notebook POSTs to the Purview Atlas bulk endpoint:

```json
{
  "entities": [
    {
      "typeName": "azure_datalake_gen2_resource_set",
      "guid": "-1",
      "attributes": {
        "qualifiedName": "onelake://a1b2c3d4-…/LH_BRONZE_LAYER/dbo/Customer",
        "name": "Customer (LH_BRONZE_LAYER)"
      }
    },
    {
      "typeName": "azure_datalake_gen2_resource_set",
      "guid": "-2",
      "attributes": {
        "qualifiedName": "onelake://a1b2c3d4-…/LH_SILVER_LAYER/dbo/Customer",
        "name": "Customer (LH_SILVER_LAYER)"
      }
    },
    {
      "typeName": "Process",
      "guid": "-3",
      "attributes": {
        "qualifiedName": "fmd://process/bronze_to_silver/a1b2c3d4-…/dbo/Customer",
        "name": "FMD Bronze To Silver: dbo.Customer",
        "description": "FMD Framework automated processing step. Pipeline run: …",
        "inputs":  [{ "guid": "-1", "typeName": "azure_datalake_gen2_resource_set" }],
        "outputs": [{ "guid": "-2", "typeName": "azure_datalake_gen2_resource_set" }],
        "columnMapping": "[{\"DatasetMapping\":[{\"Source\":\"CustomerID\",\"Sink\":\"CustomerID\"},{\"Source\":\"CustomerName\",\"Sink\":\"CustomerName\"}]}]"
      }
    }
  ]
}
```

---

---

## Prerequisites

Before configuring the integration, ensure the following are in place:

1. **Microsoft Purview account** — An active Microsoft Purview account provisioned in the same Azure tenant as your Fabric capacity.
2. **Microsoft Fabric enabled** — The Fabric tenant must have the Purview integration setting enabled.
3. **Appropriate roles**:
   - `Purview Data Curator` role in the Purview account (required to write entities via the Atlas API).
   - `Fabric Workspace Admin` or `Member` role in the FMD workspaces.
4. **Workspace Identity or Service Principal** granted the `Purview Data Curator` role so the notebook can authenticate to the Purview Atlas API using an AAD token.

> [!NOTE]
> The notebook uses `notebookutils.credentials.getToken('https://purview.azure.com')` to obtain an AAD bearer token — no passwords or client secrets are stored.

---

## Step 1 — Enable Purview Integration in Microsoft Fabric

### 1.1 Connect a Purview account to Fabric

1. Sign in to the [Microsoft Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal).
2. Navigate to **Tenant Settings → Microsoft Purview**.
3. Under **Connect a Microsoft Purview account**, toggle the setting to **Enabled**.
4. Enter the **Purview account URL** (e.g. `https://<your-account>.purview.azure.com`).
5. Click **Save**.

> [!NOTE]
> Only one Purview account can be connected to a Fabric tenant at a time.

### 1.2 Grant the Fabric Workspace Identity the Data Curator role

The notebook authenticates as the Workspace Identity. To allow it to write Atlas entities:

1. Open the [Microsoft Purview portal](https://purview.microsoft.com).
2. Navigate to **Data Map → Collections**.
3. Select the target collection (or root).
4. Click **Role assignments**.
5. Add the Fabric Workspace Identity (or Service Principal) to the **Data Curators** role.

---

## Step 2 — Register Microsoft Fabric as a Data Source in Purview

Registering Fabric workspaces in Purview enables the automatic scanner to discover tables and enrich the entities that `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW` creates via the Atlas API.

1. Open the [Microsoft Purview portal](https://purview.microsoft.com).
2. Navigate to **Data Map → Data Sources**.
3. Click **+ Register** and select **Microsoft Fabric**.
4. Register each FMD workspace:

| FMD Workspace | Contains |
|---|---|
| `<DOMAIN> DATA (D/P)` | LH_DATA_LANDINGZONE, LH_BRONZE_LAYER, LH_SILVER_LAYER |
| `<DOMAIN> CODE (D/P)` | Notebooks, Pipelines, Variable Libraries |
| `FMD_FRAMEWORK_CONFIGURATION` | SQL_FMD_FRAMEWORK database |
| Business Domain Data workspace | LH_GOLD_LAYER and reporting assets |

---

## Step 3 — Configure and Run a Scan

### 3.1 Create a scan

1. In the Purview Data Map, select the registered Fabric data source.
2. Click **New scan**.
3. Scope the scan to the lakehouses you want to catalogue (`LH_BRONZE_LAYER`, `LH_SILVER_LAYER`, etc.).
4. Choose or create a scan rule set that includes Delta Parquet file types.
5. Schedule the scan to run after each `PL_FMD_LOAD_ALL` pipeline run to keep the catalog current.

Running a scan ensures that Purview has entities for your lakehouse tables. The Atlas API calls from `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW` will then **enrich those entities** with column-level lineage on top of what the scanner already discovered.

---

## Step 4 — View Column-Level Lineage in Purview

Once `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW` has run:

1. Open the [Microsoft Purview portal](https://purview.microsoft.com).
2. Search for a table name (e.g. `Customer`).
3. Open the asset (e.g. `Customer (LH_SILVER_LAYER)`).
4. Select the **Lineage** tab.
5. Click the **Process** node (labelled `FMD Bronze To Silver: dbo.Customer`).
6. Click **View column lineage** to expand the column-to-column mapping.

The lineage graph shows:

```
LH_BRONZE_LAYER / dbo.Customer
  ├── CustomerID    ──────────────►  LH_SILVER_LAYER / dbo.Customer
  ├── CustomerName  ──────────────►    ├── CustomerID
  ├── EmailAddress  ──────────────►    ├── CustomerName
  └── ModifiedDate  ──────────────►    ├── EmailAddress
                                       └── ModifiedDate
```

---

## Step 5 — Sensitivity Labels and Classification

### Automatic classification

During each Purview scan, built-in classifiers detect sensitive data patterns (email addresses, national IDs, credit card numbers) in column names and sampled values. Classification results appear on the **Schema** tab of each asset.

### Applying sensitivity labels manually

1. Open the asset in the Purview catalog.
2. Click **Edit** (pencil icon).
3. In the **Sensitivity label** field, select the appropriate label (e.g. `Confidential`, `Highly Confidential`).
4. Click **Save**.

### Propagating labels to Power BI

Sensitivity labels applied to Fabric lakehouse tables propagate downstream to Power BI semantic models and reports built on top of that data, ensuring end-to-end data protection from lakehouse to dashboard.

---

## Purview & FMD Logging Schema

The FMD `logging` schema in `SQL_FMD_FRAMEWORK` records every pipeline and notebook execution. You can cross-reference FMD execution logs with Purview scan history and Atlas process entities to build a complete audit trail:

| FMD log table | Purview counterpart |
|---|---|
| `logging.PipelineExecution` | Purview activity log for pipeline runs |
| `logging.NotebookExecution` | Atlas `Process` entity created by `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW` |
| `logging.CopyActivityExecution` | Purview lineage edge from source to landing zone |

The `PipelineRunGuid` that appears in both the FMD log tables and the Atlas `Process` entity description links the two systems for every run.

---

## Troubleshooting

| Symptom | Likely cause | Resolution |
|---|---|---|
| `401 Unauthorized` from Purview API | Workspace Identity not in `Purview Data Curator` role | Add the Workspace Identity to the Data Curators role on the target Purview collection |
| `400 Bad Request` from Purview API | Invalid entity type or malformed `columnMapping` JSON | Check the Purview portal for type definition errors; validate the JSON payload shape |
| Column mapping is empty (`[]`) | Source and target Delta tables have no columns in common | Verify that both tables exist and have been loaded at least once before lineage registration runs |
| Lineage node not visible after run | Purview scan has not yet run or entity qualified names do not match scan results | Run a Purview scan to enrich the entities created by the Atlas API calls |
| `purview_account_name` not set | Parameter left blank in the pipeline activity | Set `purview_account_name` in the pipeline activity that calls `NB_FMD_PROCESSING_PARALLEL_MAIN_PURVIEW` |
| Processing succeeds but Purview call fails | Notebook continues and exits normally — Purview errors are non-blocking | Check the notebook output for `⚠` lines in the Purview Lineage Summary section |
| Scan fails with `Unauthorized` | Purview managed identity lacks Fabric workspace access | Add the Purview managed identity as a **Viewer** or higher on each FMD workspace |
| Duplicate assets in catalog | Multiple scans registered the same workspace | Delete duplicate data source registrations and keep only one per workspace |

---

## Related Resources

- [Microsoft Purview documentation](https://learn.microsoft.com/en-us/purview/purview)
- [Connect Microsoft Fabric to Microsoft Purview](https://learn.microsoft.com/en-us/fabric/governance/microsoft-purview-fabric-overview)
- [Microsoft Fabric data lineage in Purview](https://learn.microsoft.com/en-us/fabric/governance/lineage)
- [Apache Atlas REST API — entity bulk](https://atlas.apache.org/api/v2/resource_EntityREST.html)
- [Sensitivity labels in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/governance/sensitivity-labels-overview)
- [NB_FMD_PROCESSING_PARALLEL_MAIN](../src/NB_FMD_PROCESSING_PARALLEL_MAIN.Notebook/notebook-content.py) — the base parallel processing notebook
- [FMD Framework deployment guide](../FMD_FRAMEWORK_DEPLOYMENT.md)
- [FMD Framework documentation](https://erwindekreuk.com/fmd-framework/)
- [FMD Wiki](https://github.com/edkreuk/FMD_FRAMEWORK/wiki)
