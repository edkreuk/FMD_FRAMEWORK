# Microsoft Purview Integration with the FMD Framework

## Overview

[Microsoft Purview](https://learn.microsoft.com/en-us/purview/purview) is a unified data governance solution that provides data discovery, classification, lineage, and compliance capabilities across your entire data estate. When used alongside the **Fabric Metadata-Driven Framework (FMD)**, Purview gives data teams a complete picture of where data comes from, how it flows through the Medallion layers, and how it is classified — all from a single portal.

This page describes how to connect Microsoft Fabric to Microsoft Purview, scan your FMD lakehouses, view end-to-end data lineage, and apply sensitivity labels to data assets.

---

## Why Use Purview with FMD?

| Capability | Benefit |
|---|---|
| **Automated data discovery** | Purview automatically scans and catalogues Delta tables in Landing Zone, Bronze, Silver, and Gold lakehouses |
| **End-to-end lineage** | Lineage is captured from source system → Landing Zone → Bronze → Silver → Gold, including notebook and pipeline runs |
| **Sensitivity classification** | Purview classifiers detect PII, financial data, and other sensitive patterns across your Delta tables |
| **Unified data catalog** | Business users can search for and understand datasets without needing direct Fabric access |
| **Compliance and auditing** | Purview Compliance Manager and audit logs complement the FMD `logging` schema for full regulatory traceability |

---

## Prerequisites

Before configuring the integration, ensure the following are in place:

1. **Microsoft Purview account** — An active Microsoft Purview account provisioned in the same Azure tenant as your Fabric capacity.
2. **Microsoft Fabric enabled** — The Fabric tenant must have the Purview integration setting enabled.
3. **Appropriate roles**:
   - `Purview Data Curator` or `Purview Data Reader` role in the Purview account (to scan and browse assets).
   - `Fabric Workspace Admin` or `Member` role in the FMD workspaces to allow Purview to scan them.
4. **Managed Identity or Service Principal** configured in both Purview and Fabric with the required permissions.

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

### 1.2 Enable sensitivity labels (optional)

1. In the Fabric Admin Portal, go to **Tenant Settings → Information Protection**.
2. Enable **Apply sensitivity labels to content**.
3. Ensure your Purview account has sensitivity labels published for the tenant via the Microsoft Purview Compliance Portal.

---

## Step 2 — Register Microsoft Fabric as a Data Source in Purview

1. Open the [Microsoft Purview portal](https://purview.microsoft.com).
2. Navigate to **Data Map → Data Sources**.
3. Click **+ Register**.
4. Search for and select **Microsoft Fabric**.
5. Fill in the required details:

| Field | Value |
|---|---|
| Name | A descriptive name (e.g. `FMD_FABRIC`) |
| Tenant ID | Your Azure AD tenant ID |
| Fabric workspace | Select the FMD workspaces to register (see table below) |

Register each FMD workspace that you want to catalogue:

| FMD Workspace | Contains |
|---|---|
| `<DOMAIN> DATA (D/P)` | LH_DATA_LANDINGZONE, LH_BRONZE_LAYER, LH_SILVER_LAYER |
| `<DOMAIN> CODE (D/P)` | Notebooks, Pipelines, Variable Libraries |
| `FMD_FRAMEWORK_CONFIGURATION` | SQL_FMD_FRAMEWORK database |
| Business Domain Data workspace | LH_GOLD_LAYER and reporting assets |

6. Click **Register** to save the data source.

---

## Step 3 — Configure and Run a Scan

### 3.1 Create a scan

1. In the Purview Data Map, select the registered Fabric data source.
2. Click **New scan**.
3. Provide a **scan name** (e.g. `SCAN_FMD_BRONZE_SILVER`).
4. Select the **scope**: choose the specific lakehouses you want to scan (e.g. `LH_BRONZE_LAYER`, `LH_SILVER_LAYER`).
5. Choose or create a **scan rule set** — use the default Microsoft Fabric rule set or create a custom set that includes the file types used by FMD (Delta Parquet).
6. Select an **integration runtime** — use the default AutoResolveIntegrationRuntime for Fabric-native assets.

### 3.2 Set a scan trigger

Purview supports both **one-time** and **recurring** scans. For FMD, a recurring scan aligned to your pipeline schedule is recommended:

- After each `PL_FMD_LOAD_ALL` run, new Delta tables or updated partitions may appear in Bronze/Silver.
- A daily or post-pipeline trigger keeps the catalog up to date.

### 3.3 Run the scan

Click **Save and run**. The first scan may take several minutes depending on the number of tables. Monitor progress in **Scan History**.

---

## Step 4 — View Data Assets in the Purview Catalog

Once the scan completes, all Delta tables and other Fabric items are visible in the Purview Data Catalog.

### Browsing by Medallion layer

Use the **Browse assets** view and filter by:

- **Source type**: Microsoft Fabric
- **Lakehouse**: filter by `LH_BRONZE_LAYER`, `LH_SILVER_LAYER`, or `LH_GOLD_LAYER`
- **Classification**: filter by PII, financial, or custom classification labels applied during the scan

### Searching for a specific entity

Use the Purview search bar to find an entity by its table name. For example, searching for `Customer` returns:

- `LH_BRONZE_LAYER.dbo.Customer` — Bronze Delta table
- `LH_SILVER_LAYER.dbo.Customer` — Silver SCD Type 2 table
- `LH_GOLD_LAYER.gold.DimCustomer` — Gold dimension table (if a Business Domain is deployed)

---

## Step 5 — Data Lineage

Microsoft Purview automatically captures lineage for Fabric pipelines and notebooks that move or transform data.

### Lineage captured automatically

The following FMD artifacts emit lineage to Purview when they run:

| FMD Artifact | Lineage captured |
|---|---|
| `PL_FMD_LDZ_COPY_FROM_*` | Source system → LH_DATA_LANDINGZONE |
| `NB_FMD_LOAD_LANDING_BRONZE` | LH_DATA_LANDINGZONE → LH_BRONZE_LAYER |
| `NB_FMD_LOAD_BRONZE_SILVER` | LH_BRONZE_LAYER → LH_SILVER_LAYER |
| `NB_LOAD_GOLD` (Business Domain) | LH_SILVER_LAYER → LH_GOLD_LAYER |

> [!NOTE]
> Lineage from Fabric pipelines and notebooks is captured automatically in Purview when the Fabric–Purview integration is enabled. No additional configuration inside FMD notebooks is required.

### Viewing lineage in Purview

1. Open a data asset in the Purview catalog (e.g. `LH_SILVER_LAYER.dbo.Customer`).
2. Select the **Lineage** tab.
3. Use the interactive graph to trace data back to its source system and forward to the Gold layer and reporting datasets.

---

## Step 6 — Sensitivity Labels and Classification

### Automatic classification

During each scan, Purview applies built-in classifiers to detect sensitive data patterns (e.g. email addresses, national IDs, credit card numbers) in column names and sampled values.

Classification results are visible on each asset's **Schema** tab.

### Applying sensitivity labels manually

If a table or column is not automatically classified, you can apply a sensitivity label manually:

1. Open the asset in the Purview catalog.
2. Click **Edit** (pencil icon).
3. In the **Sensitivity label** field, select the appropriate label (e.g. `Confidential`, `Highly Confidential`).
4. Click **Save**.

### Propagating labels to Power BI

Sensitivity labels applied to Fabric lakehouse tables propagate downstream to Power BI semantic models and reports when the report is built on top of that data. This ensures end-to-end data protection from lakehouse to dashboard.

---

## Purview & FMD Logging Schema

The FMD `logging` schema in `SQL_FMD_FRAMEWORK` records every pipeline and notebook execution. You can cross-reference FMD execution logs with Purview scan history and lineage events to produce a complete audit trail:

| FMD log table | Purview counterpart |
|---|---|
| `logging.PipelineExecution` | Purview activity log for pipeline runs |
| `logging.NotebookExecution` | Purview lineage node for notebook runs |
| `logging.CopyActivityExecution` | Purview lineage edge from source to landing zone |

---

## Troubleshooting

| Symptom | Likely cause | Resolution |
|---|---|---|
| Scan fails with `Unauthorized` | Purview managed identity lacks Fabric workspace access | Add the Purview managed identity as a **Viewer** or higher on each FMD workspace |
| Assets not appearing after scan | Scan scope excludes the target lakehouse | Edit the scan and expand the scope to include all lakehouses |
| Lineage not showing for notebooks | Fabric–Purview integration not enabled at tenant level | Enable the Purview integration in the Fabric Admin Portal (Step 1) |
| Sensitivity labels not visible | Labels not published in Compliance Portal | Publish sensitivity labels via the Microsoft Purview Compliance Portal and wait for replication (up to 24 hours) |
| Duplicate assets in catalog | Multiple scans registered the same workspace | Delete duplicate data source registrations and keep only one per workspace |
| Scan times out on large lakehouses | Too many Delta table partitions | Narrow the scan scope to specific schemas or tables, or schedule scans during off-peak hours |

---

## Related Resources

- [Microsoft Purview documentation](https://learn.microsoft.com/en-us/purview/purview)
- [Connect Microsoft Fabric to Microsoft Purview](https://learn.microsoft.com/en-us/fabric/governance/microsoft-purview-fabric-overview)
- [Microsoft Fabric data lineage in Purview](https://learn.microsoft.com/en-us/fabric/governance/lineage)
- [Sensitivity labels in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/governance/sensitivity-labels-overview)
- [FMD Framework deployment guide](../FMD_FRAMEWORK_DEPLOYMENT.md)
- [FMD Framework documentation](https://erwindekreuk.com/fmd-framework/)
- [FMD Wiki](https://github.com/edkreuk/FMD_FRAMEWORK/wiki)
