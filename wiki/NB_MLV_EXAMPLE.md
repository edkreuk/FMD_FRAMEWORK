# NB_MLV_EXAMPLE — Create Materialized Lake Views (Template)

## Overview

`NB_MLV_EXAMPLE` is a **SparkSQL template notebook** for creating [Materialized Lake Views (MLVs)](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-materialized-view) on top of Gold layer lakehouse tables. It acts as a starting point — copy and adapt it for each business domain that needs MLVs.

> For a working example with real SQL, see the demo implementation notebook `NB_MLV_DEMO_GOLD` in the same business domain folder.

---

## What is a Materialized Lake View?

A Materialized Lake View is a pre-computed, persisted result of a SQL SELECT statement stored in the lakehouse. Unlike a regular view, the query result is **materialized** (physically stored), making it faster to read for downstream reporting tools such as Power BI.

In the FMD Framework, MLVs are typically created on top of shortcuts or Delta tables in the **Gold lakehouse** to serve the semantic / reporting layer.

---

## Prerequisites

1. **Gold lakehouse deployed** — the Business Domain setup (`NB_SETUP_BUSINESS_DOMAINS.ipynb`) must have run successfully.
2. **Source tables available** — the Silver or Gold tables referenced in your SELECT statements must exist and be populated.
3. **Default lakehouse attached** — the notebook must have the **Gold lakehouse** set as its default lakehouse before running (see [Attaching the Lakehouse](#attaching-the-lakehouse)).

---

## Attaching the Lakehouse

Before running, attach the correct Gold lakehouse as the default lakehouse of the notebook:

1. Open the notebook in your Business Domain **Code workspace**.
2. In the top-right lakehouse panel, click **Add lakehouse**.
3. Select your Gold lakehouse (e.g. `LH_GOLD_LAYER`) from the Business Domain **Data workspace**.
4. Set it as the **default** lakehouse.

This makes unqualified table references (e.g. `gold.FactOrderLines`) resolve to your Gold lakehouse automatically.

---

## How to Use This Notebook

### Step 1 — Create a schema (optional)

If your MLVs should live in a named schema, add a cell to create it first:

```sql
CREATE SCHEMA IF NOT EXISTS gold
```

### Step 2 — Add MLV definitions

Replace the placeholder comment in the code cell with one or more `CREATE MATERIALIZED LAKE VIEW` statements:

```sql
CREATE MATERIALIZED LAKE VIEW <mlv_name>
AS
SELECT ...
FROM   <source_table>
WHERE  ...
```

**Syntax rules:**
- Use `CREATE OR REPLACE MATERIALIZED LAKE VIEW` to allow re-running the notebook safely (idempotent).
- Qualify source tables with the lakehouse name when joining across lakehouses, e.g. `LH_GOLD_LAYER.dbo.Sales_Orders`.
- Each `CREATE` statement should be in its own notebook cell for clarity and easier debugging.

### Step 3 — Run the notebook

Select **Run all** from the toolbar. Each cell executes the corresponding `CREATE` statement against the attached lakehouse.

### Step 4 — Refresh the MLV graph

Once the notebook completes:

1. Navigate to the Gold lakehouse in Fabric.
2. Open the **Materialized lake views** section.
3. Click **Refresh** to see the newly created views.

---

## Demo Example

The notebook `NB_MLV_DEMO_GOLD` in the same folder shows a complete implementation with the following MLVs built on the demo dataset:

| MLV name | Description |
|---|---|
| `gold.FactOrderLines` | Order lines joined with order headers |
| `gold.DimOrders` | Order dimension (non-sensitive columns) |
| `gold.DimCustomer` | Customer dimension joined with buying groups |
| `gold.DimPackageType` | Package type lookup |
| `gold.DimStockItems` | Stock item dimension |

Use this as a reference when writing your own MLV definitions.

---

## Naming Conventions

Follow these conventions to keep the Gold layer consistent:

| Pattern | Example |
|---|---|
| Fact tables | `gold.Fact<Subject>` |
| Dimension tables | `gold.Dim<Subject>` |
| Schema | `gold` (lower-case) |

---

## Troubleshooting

| Symptom | Likely cause | Resolution |
|---|---|---|
| `Table or view not found` | Source table does not exist | Confirm shortcuts or Delta tables are created in the Gold lakehouse (run `NB_CREATE_SHORTCUTS` first) |
| `No default lakehouse` | Lakehouse not attached | Attach the Gold lakehouse as default (see [Attaching the Lakehouse](#attaching-the-lakehouse)) |
| MLV not visible after run | Lakehouse view not refreshed | Click **Refresh** in the Materialized lake views section of the lakehouse |
| `Syntax error` | Unsupported SQL syntax | MLVs support a subset of SparkSQL — avoid window functions and subqueries in the top-level SELECT |

---

## Related Resources

- [FMD Business Domain Deployment Guide](../FMD_BUSINESS_DOMAIN_DEPLOYMENT.md)
- [NB_CREATE_SHORTCUTS](NB_CREATE_SHORTCUTS.md) — create the OneLake shortcuts that feed Gold tables
- [Microsoft Fabric — Materialized Lake Views documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-materialized-view)
- [FMD Framework documentation](https://erwindekreuk.com/fmd-framework/)
