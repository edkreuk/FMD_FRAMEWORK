# Using OneLake Tables as a Source in FMD Framework

The FMD Framework ships with built-in support for reading Delta tables from a OneLake / Lakehouse source and ingesting them through the standard Landing Zone → Bronze → Silver pipeline. This page walks you through the six steps required to configure and run this flow.

---

## Overview

The following components are already deployed as part of the framework:

| Component | Role |
|---|---|
| `PL_FMD_LDZ_COMMAND_ONELAKE` | Orchestrator — queries `execution.vw_LoadSourceToLandingzone` for all `ONELAKE` connections and dispatches by datasource type |
| `PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01` | Copy pipeline — reads a Delta table from the source lakehouse and writes a Parquet file to the Landing Zone |
| `integration.sp_UpsertConnection` | Registers a connection record |
| `integration.sp_UpsertDataSource` | Registers a data source record |
| `integration.sp_UpsertLandingzoneBronzeSilver` | Atomically creates LandingzoneEntity, BronzeLayerEntity, and SilverLayerEntity rows |

You only need to register the correct metadata in `SQL_FMD_FRAMEWORK`; the pipelines are already wired together.

---

## Step 1 — Create a Fabric OneLake connection

In the **Fabric portal**, create (or reuse) a **OneLake** connection that points to the source lakehouse.

1. Go to **Settings → Connections** (or use the Data Factory / Pipeline connector picker).
2. Create a new connection of type **OneLake** and point it at the source lakehouse.
3. Copy the **Connection GUID** — you will need it in Step 2.

> **Important:** Use `'ONELAKE'` as the connection type. The view `execution.vw_LoadSourceToLandingzone` surfaces this as `ConnectionType`, which is the filter used by `PL_FMD_LDZ_COMMAND_ONELAKE` to pick up entities for processing.

---

## Step 2 — Register the Connection

Execute the following in `SQL_FMD_FRAMEWORK`. Replace the placeholder values with your actual values.

```sql
EXEC [integration].[sp_UpsertConnection]
    @ConnectionGuid  = '<your-onelake-connection-guid>',   -- GUID from Step 1
    @Name            = 'CONN_ONELAKE_<SOURCE_LAKEHOUSE_NAME>',
    @Type            = 'ONELAKE',
    @IsActive        = 1;
```

> The procedure performs an upsert (INSERT if the GUID doesn't exist, UPDATE otherwise).

Note the `ConnectionId` returned — you need it in Step 3. You can also retrieve it with:

```sql
SELECT ConnectionId FROM [integration].[Connection]
WHERE ConnectionGuid = '<your-onelake-connection-guid>';
```

---

## Step 3 — Register the DataSource

```sql
EXEC [integration].[sp_UpsertDataSource]
    @ConnectionId  = <ConnectionId from Step 2>,
    @Name          = '<source lakehouse name, e.g. LH_GOLD_SALES>',
    @Namespace     = '<short namespace, e.g. SALES>',       -- max 10 characters
    @Type          = 'ONELAKE_TABLES_01',
    @Description   = 'OneLake Tables source for <source lakehouse>',
    @IsActive      = 1;
```

The procedure returns a `DataSourceId` result set. Note this value for Step 4.

> **`@Type = 'ONELAKE_TABLES_01'`** is the exact string matched by the `Switch` activity inside `PL_FMD_LDZ_COMMAND_ONELAKE`. It dispatches to `PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01`. Do not change this value.

> **`@Namespace`** is used by `vw_LoadSourceToLandingzone` to build the target file path:  
> `<FilePath>/<Namespace>/<yyyy>/<MM>/<dd>/`

---

## Step 4 — Register entities (Landing Zone → Bronze → Silver)

For **each Delta table** you want to ingest, call the all-in-one stored procedure. It atomically creates rows in `LandingzoneEntity`, `BronzeLayerEntity`, and `SilverLayerEntity` within a single transaction.

```sql
EXEC [integration].[sp_UpsertLandingzoneBronzeSilver]
    @DataSourceId        = <DataSourceId from Step 3>,
    @WorkspaceGuid       = '<Data workspace GUID>',         -- workspace owning LH_DATA_LANDINGZONE, LH_BRONZE_LAYER, LH_SILVER_LAYER
    @SourceSchema        = '<schema of source table, e.g. dbo>',
    @SourceName          = '<source table name, e.g. FactSales>',
    @TargetSchema        = '<target schema in Bronze/Silver, e.g. sales>',
    @TargetName          = '<target table name, e.g. fact_sales>',
    @SourceCustomSelect  = NULL,                            -- or a custom SELECT statement
    @FileName            = '<base parquet file name, e.g. fact_sales>',
    @FilePath            = 'onelake_tables',                -- relative path inside Landing Zone Files
    @FileType            = 'parquet',
    @IsIncremental       = 0,                               -- 1 for incremental, 0 for full load
    @IsIncrementalColumn = NULL,                            -- e.g. 'ModifiedDate' if incremental
    @CustomNotebookName  = NULL,
    @PrimaryKeys         = '<comma-separated PKs, e.g. SalesId>';
```

The procedure returns a result set with `LandingzoneEntityId`, `BronzeLayerEntityId`, and `SilverLayerEntityId`.

Repeat this call for every source table.

### Parameter reference

| Parameter | Required | Description |
|---|---|---|
| `@DataSourceId` | ✅ | ID from Step 3 |
| `@WorkspaceGuid` | ✅ | GUID of the **Data** workspace (the one containing `LH_DATA_LANDINGZONE`, `LH_BRONZE_LAYER`, `LH_SILVER_LAYER`) |
| `@SourceSchema` | ✅ | Schema of the source Delta table (e.g. `dbo`) |
| `@SourceName` | ✅ | Name of the source Delta table (e.g. `FactSales`) |
| `@TargetSchema` | ✅ | Schema to use in Bronze/Silver (e.g. `sales`) |
| `@TargetName` | ✅ | Table name in Bronze/Silver (e.g. `fact_sales`) |
| `@SourceCustomSelect` | ❌ | Custom SQL SELECT for the source (leave `NULL` for `SELECT *`) |
| `@FileName` | ✅ | Base name for the Parquet file written to Landing Zone (without extension) |
| `@FilePath` | ✅ | Relative folder path inside `LH_DATA_LANDINGZONE/Files/` — use `onelake_tables` |
| `@FileType` | ✅ | File format — use `parquet` |
| `@IsIncremental` | ✅ | `0` = full load on every run; `1` = load only new/changed rows |
| `@IsIncrementalColumn` | ❌ | Column name used for incremental watermark (e.g. `ModifiedDate`) |
| `@CustomNotebookName` | ❌ | Override notebook for Bronze processing (leave `NULL` for default) |
| `@PrimaryKeys` | ✅ | Comma-separated primary key column(s) used for Bronze merge (e.g. `SalesId` or `OrderId,LineId`) |

> The stored procedure automatically looks up `LH_DATA_LANDINGZONE`, `LH_BRONZE_LAYER`, and `LH_SILVER_LAYER` lakehouse IDs from `integration.Lakehouse` using the supplied `@WorkspaceGuid`.

---

## Step 5 — Configure the source Lakehouse in the copy pipeline

The copy pipeline `PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01` currently has the source lakehouse **hard-coded** in the `CP_SOURCE_datalandingzone` copy activity under `source.datasetSettings.linkedService.typeProperties`.

Open the pipeline in the **Fabric portal** and update the source linked service to point to **your** source lakehouse:

| Property | Value |
|---|---|
| `workspaceId` | GUID of the workspace that contains the source lakehouse |
| `artifactId` | GUID of the source lakehouse itself |

### Source table naming convention

The copy pipeline resolves the source table name dynamically using:

```
@concat(item().SourceSchema, '_', item().SourceName)
```

This means your source Delta table **must be named** `<SourceSchema>_<SourceName>` in the source lakehouse.

**Example:** If you registered `@SourceSchema = 'dbo'` and `@SourceName = 'FactSales'`, the pipeline will read the table `dbo_FactSales` from the source lakehouse.

---

## Step 6 — Run the pipeline

Trigger **`PL_FMD_LOAD_LANDINGZONE`** (or **`PL_FMD_LOAD_ALL`**) with the parameter:

| Parameter | Value |
|---|---|
| `Data_WorkspaceGuid` | GUID of the **Data** workspace used in Step 4 |

### Orchestration flow

```
PL_FMD_LOAD_LANDINGZONE
  └─► PL_FMD_LDZ_COMMAND_ONELAKE
        ├─ Queries execution.vw_LoadSourceToLandingzone
        │    WHERE ConnectionType = 'ONELAKE'
        │    AND   WorkspaceGuid  = <Data_WorkspaceGuid>
        └─ Switch on DatasourceType
             └─ ONELAKE_TABLES_01
                  └─► PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01
                        ├─ Reads Delta table from source lakehouse
                        └─ Writes Parquet to LH_DATA_LANDINGZONE/Files/<FilePath>/<Namespace>/<yyyy>/<MM>/<dd>/
```

After the Landing Zone load completes:
- `PL_FMD_LOAD_BRONZE` processes the Parquet files into `LH_BRONZE_LAYER` Delta tables (with hash-based change tracking and the supplied primary keys).
- `PL_FMD_LOAD_SILVER` applies SCD Type 2 merge logic into `LH_SILVER_LAYER`.

---

## Complete example — Sales fact table

The following example registers a single table `dbo_FactSales` from a Gold lakehouse named `LH_GOLD_SALES`.

```sql
-- Step 2: Register the connection
EXEC [integration].[sp_UpsertConnection]
    @ConnectionGuid  = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
    @Name            = 'CONN_ONELAKE_LH_GOLD_SALES',
    @Type            = 'ONELAKE',
    @IsActive        = 1;

-- Retrieve ConnectionId
DECLARE @ConnectionId INT;
SELECT @ConnectionId = ConnectionId
FROM [integration].[Connection]
WHERE ConnectionGuid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';

-- Step 3: Register the data source
EXEC [integration].[sp_UpsertDataSource]
    @ConnectionId  = @ConnectionId,
    @Name          = 'LH_GOLD_SALES',
    @Namespace     = 'SALES',
    @Type          = 'ONELAKE_TABLES_01',
    @Description   = 'OneLake Tables source for LH_GOLD_SALES',
    @IsActive      = 1;

-- Retrieve DataSourceId
DECLARE @DataSourceId INT;
SELECT @DataSourceId = DataSourceId
FROM [integration].[DataSource]
WHERE [Name] = 'LH_GOLD_SALES' AND [Type] = 'ONELAKE_TABLES_01';

-- Step 4: Register FactSales (full load, single PK)
EXEC [integration].[sp_UpsertLandingzoneBronzeSilver]
    @DataSourceId        = @DataSourceId,
    @WorkspaceGuid       = 'ffffffff-0000-1111-2222-333333333333',   -- SALES DATA (D) workspace
    @SourceSchema        = 'dbo',
    @SourceName          = 'FactSales',
    @TargetSchema        = 'sales',
    @TargetName          = 'fact_sales',
    @SourceCustomSelect  = NULL,
    @FileName            = 'fact_sales',
    @FilePath            = 'onelake_tables',
    @FileType            = 'parquet',
    @IsIncremental       = 0,
    @IsIncrementalColumn = NULL,
    @CustomNotebookName  = NULL,
    @PrimaryKeys         = 'SalesId';
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Pipeline picks up no entities | `ConnectionType` is not `'ONELAKE'` | Re-run `sp_UpsertConnection` with `@Type = 'ONELAKE'` |
| Switch activity takes default branch | `DatasourceType` is not exactly `'ONELAKE_TABLES_01'` | Re-run `sp_UpsertDataSource` with `@Type = 'ONELAKE_TABLES_01'` |
| Copy activity fails with "table not found" | Source Delta table is not named `<SourceSchema>_<SourceName>` | Rename the source Delta table or adjust `@SourceSchema` / `@SourceName` registration |
| Copy activity fails with "lakehouse not found" | `workspaceId` / `artifactId` in the pipeline source linked service are wrong | Open `PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01` in the Fabric portal and update the source linked service GUIDs |
| `sp_UpsertLandingzoneBronzeSilver` fails with "lakehouse not found" | `@WorkspaceGuid` doesn't match a workspace registered in `integration.Lakehouse` | Verify the workspace GUID with `SELECT * FROM [integration].[Lakehouse]` |
| Incremental load always loads all rows | `@IsIncrementalColumn` is NULL or the watermark is missing | Ensure the column exists in the source table and a row exists in `execution.LandingzoneEntityLastLoadValue` |
