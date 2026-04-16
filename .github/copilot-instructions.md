# Copilot Instructions — FMD Framework

## What this repository is

The **Fabric Metadata-Driven Framework (FMD)** is an open-source Microsoft Fabric framework that automates, orchestrates, and standardises metadata-driven data pipelines. It targets a **Lakehouse-first / Medallion architecture** (Landing Zone → Bronze → Silver → Gold) and is fully deployed into Microsoft Fabric using two Jupyter deployment notebooks.

Key resources:
- Full docs: <https://erwindekreuk.com/fmd-framework/>
- Wiki: <https://github.com/edkreuk/FMD_FRAMEWORK/wiki>
- Deployment guide: [`FMD_FRAMEWORK_DEPLOYMENT.md`](../FMD_FRAMEWORK_DEPLOYMENT.md)
- Business Domain guide: [`FMD_BUSINESS_DOMAIN_DEPLOYMENT.md`](../FMD_BUSINESS_DOMAIN_DEPLOYMENT.md)

---

## Repository layout

```
/
├── .github/                      # GitHub metadata (issue templates, this file)
├── Images/                       # Documentation images
├── Taskflow/                     # Fabric Taskflow JSON + assignment diagram
├── config/                       # Environment-specific config (item_config.yaml)
├── demodata/                     # Sample CSV files (e.g. customer.csv)
├── setup/                        # Deployment notebooks (run these in Fabric)
│   ├── NB_SETUP_FMD.ipynb           – deploys the FMD Framework workspaces & artifacts
│   └── NB_SETUP_BUSINESS_DOMAINS.ipynb – deploys Business Domain workspaces
└── src/                          # All Fabric artifacts (stored as source files)
    ├── Config_Database/          – SQL Database definition (schemas: integration, execution, logging)
    ├── ENV_FMD.Environment/      – Fabric Environment (Python libraries, Spark config)
    ├── NB_*.Notebook/            – PySpark notebooks (notebook-content.py + .platform)
    ├── PL_*.DataPipeline/        – Fabric Data Pipelines (JSON)
    ├── SQL_FMD_FRAMEWORK.SQLDatabase/ – SQL project (.sqlproj)
    ├── VAR_CONFIG_FMD.VariableLibrary/ – runtime config variables
    ├── VAR_FMD.VariableLibrary/  – default framework variables
    └── business_domain/          – Gold-layer notebooks + variable libraries
```

---

## Architecture overview

### Workspace separation
The framework uses **three separate Fabric workspaces**:

| Workspace | Naming convention | Purpose |
|---|---|---|
| Code | `<DOMAIN> CODE (D/P)` | Notebooks, pipelines, variable libraries |
| Data | `<DOMAIN> DATA (D/P)` | Lakehouses (LH_DATA_LANDINGZONE, LH_BRONZE_LAYER, LH_SILVER_LAYER) |
| Configuration | `FMD_FRAMEWORK_CONFIGURATION` | SQL_FMD_FRAMEWORK database, deployment notebooks |

Business Domains add two more: a Gold/Reporting workspace and a separate Business Domain workspace.

### Medallion layers

| Layer | Lakehouse | Description |
|---|---|---|
| Landing Zone | `LH_DATA_LANDINGZONE` | Raw files ingested from source systems |
| Bronze | `LH_BRONZE_LAYER` | Cleansed Delta tables with hash-based change tracking |
| Silver | `LH_SILVER_LAYER` | SCD Type 2 Delta tables (full history) |
| Gold | Domain-specific lakehouse | Aggregated / modelled data for reporting |

### Pipeline naming convention
| Prefix | Meaning |
|---|---|
| `PL_FMD_LDZ_COMMAND_*` | Commands a data copy from a specific source type |
| `PL_FMD_LDZ_COPY_FROM_*` | Actual copy pipeline for a source type |
| `PL_FMD_LOAD_*` | Orchestration pipeline (LANDINGZONE / BRONZE / SILVER / ALL) |

---

## Configuration database (`SQL_FMD_FRAMEWORK`)

All pipeline metadata is stored in the **Fabric SQL Database**. The three schemas are:

### `integration` schema — what to load
| Table | Key columns |
|---|---|
| `DataSource` | Source connection metadata |
| `Connection` | Fabric connection IDs |
| `Workspace` | Workspace GUIDs |
| `Lakehouse` | Lakehouse GUIDs per workspace/layer |
| `LandingzoneEntity` | Source entity config (SourceName, FilePath, FileType, IsIncremental, …) |
| `BronzeLayerEntity` | Bronze target config (PrimaryKeys, CleansingRules, …) |
| `SilverLayerEntity` | Silver target config (CleansingRules, …) |
| `Pipeline` | Pipeline metadata |

**Important stored procedure**: `integration.sp_UpsertLandingzoneBronzeSilver` registers a full LDZ → Bronze → Silver entity in a single transactional call.

### `execution` schema — what is queued/running
| Table | Purpose |
|---|---|
| `PipelineLandingzoneEntity` | Files detected in landing zone, pending processing |
| `PipelineBronzeLayerEntity` | Bronze entities queued for processing |
| `LandingzoneEntityLastLoadValue` | Watermark for incremental loads |

### `logging` schema — audit trail
| Table | Key columns |
|---|---|
| `PipelineExecution` | WorkspaceGuid, PipelineRunGuid, EntityId, EntityLayer, LogType, LogData |
| `NotebookExecution` | Notebook-level audit rows |
| `CopyActivityExecution` | Copy activity metrics |

---

## Notebooks

All notebooks live in `src/NB_*.Notebook/` and are saved as `notebook-content.py` (PySpark, kernel `synapse_pyspark`).

| Notebook | Role |
|---|---|
| `NB_FMD_UTILITY_FUNCTIONS` | Shared helper functions — always referenced via `%run` or `notebookutils.notebook.run`. Contains `execute_with_outputs` (pyodbc + AAD token) and `build_exec_statement`. |
| `NB_FMD_LOAD_LANDING_BRONZE` | Reads Landing Zone files → applies DQ + cleansing → writes Bronze Delta |
| `NB_FMD_LOAD_BRONZE_SILVER` | Bronze → Silver SCD Type 2 merge |
| `NB_FMD_DQ_CLEANSING` | Applies framework cleansing rules |
| `NB_FMD_CUSTOM_DQ_CLEANSING` | User-extensible cleansing notebook |
| `NB_FMD_PROCESSING_LANDINGZONE_MAIN` | Wrapper that invokes custom extraction notebooks |
| `NB_FMD_PROCESSING_PARALLEL_MAIN` | Parallel orchestration of Bronze/Silver loads |
| `NB_UTILITIES_SETUP_FMD` | One-time setup utilities |
| `business_domain/NB_LOAD_GOLD` | Loads data into Gold layer |
| `business_domain/NB_CREATE_DIMDATE` | Creates dimension date table |
| `business_domain/NB_CREATE_SHORTCUTS` | Creates OneLake shortcuts |

### Notebook conventions
- Settings are loaded at the top of every notebook via:
  ```python
  config_settings = notebookutils.variableLibrary.getLibrary("VAR_CONFIG_FMD")
  default_settings = notebookutils.variableLibrary.getLibrary("VAR_FMD")
  ```
- SQL connections use **AAD token authentication** via pyodbc (no passwords stored):
  ```python
  token = notebookutils.credentials.getToken('https://analysis.windows.net/powerbi/api').encode("UTF-16-LE")
  token_struct = struct.pack(f'<I{len(token)}s', len(token), token)
  conn = pyodbc.connect(f"DRIVER={driver};SERVER={connstring};...", attrs_before={1256: token_struct})
  ```
- ODBC driver default: `{ODBC Driver 18 for SQL Server}`.
- Notebooks are parameterised — parameters are declared in a dedicated `# PARAMETERS CELL` section.

---

## Variable Libraries

| Library | File | Purpose |
|---|---|---|
| `VAR_CONFIG_FMD` | `src/VAR_CONFIG_FMD.VariableLibrary/variables.json` | Runtime config: DB connection string, DB name, workspace/database GUIDs |
| `VAR_FMD` | `src/VAR_FMD.VariableLibrary/variables.json` | Default flags: Key Vault URIs, `lakehouse_schema_enabled` |

`VAR_CONFIG_FMD` key variables:
- `fmd_fabric_db_connection` — ODBC connection string to SQL_FMD_FRAMEWORK
- `fmd_fabric_db_name` — database name
- `fmd_config_workspace_guid` — config workspace GUID
- `fmd_config_database_guid` — SQL database GUID

---

## Deployment

### FMD Framework
1. Download `setup/NB_SETUP_FMD.ipynb` and import it into a Fabric workspace.
2. Edit the configuration cell (capacity, domain names, workspace roles, connection IDs).
3. Run the notebook — it provisions all workspaces, lakehouses, pipelines, and notebooks from this repo automatically (reads from `repo_owner="edkreuk"`, `repo_name="FMD_FRAMEWORK"`, `branch="main"`).

### Business Domains
1. Download `setup/NB_SETUP_BUSINESS_DOMAINS.ipynb` and import it into a Fabric workspace.
2. Edit settings (business_domain_names, configuration_database_workspace, etc.).
3. Run the notebook.

### Config files
- `config/item_config.yaml` — maps workspace names to GUIDs and connection IDs for a deployed environment (do **not** commit real GUIDs for shared/production environments).
- `config/item_deployment.json`, `config/item_initial_setup.json`, etc. — deployment manifests used by the setup notebooks.

---

## How to add a new data source (integration entity)

1. Register a Data Source in `integration.DataSource`.
2. Call `integration.sp_UpsertLandingzoneBronzeSilver` with the source/target metadata and primary keys. This atomically creates `LandingzoneEntity`, `BronzeLayerEntity`, and `SilverLayerEntity` rows.
3. The pipeline `PL_FMD_LOAD_LANDINGZONE` will pick up the new entity on the next run.

---

## SQL Database project conventions

- Project file: `src/SQL_FMD_FRAMEWORK.SQLDatabase/SQL_FMD_FRAMEWORK.sqlproj`
- Schema folders: `integration/`, `execution/`, `logging/`, `Security/`
- Each object lives in its own `.sql` file named after the object.
- Stored procedures use `WITH EXECUTE AS CALLER`.
- All upsert procedures follow the pattern `sp_Upsert<ObjectName>` and use `MERGE` or `IF NOT EXISTS / UPDATE` patterns.
- `GO` statement always appears at the end of each file.

---

## Common pitfalls & workarounds

| Issue | Workaround |
|---|---|
| Fabric Admin role needed for domain creation | Set `create_domains = False` in the setup notebook if the executing identity is not a Fabric Admin. |
| Workspace Identity vs. Service Principal | Workspace Identity is auto-assigned as Contributor; Service Principals must be manually added. Use Object ID from **Enterprise Applications** in Entra ID, not the App Registration Object ID. |
| Spark session timeout during long deployments | Set Spark session timeout ≥ 1 hour in Workspace Settings → Data Engineering → Jobs. |
| ODBC driver version | Default is `{ODBC Driver 18 for SQL Server}`. Change `driver` variable in setup notebook if a different driver is installed. |
| Variable Library overwritten on re-deploy | Set `overwrite_variable_library = False` to preserve custom changes. |
| `VAR_CONFIG_FMD` empty after fresh deploy | Run the setup notebook first; it populates the variable library with the correct GUIDs and connection strings. |

---

## Contributing workflow

1. Fork the repo and create a feature branch from `main`.
2. Make changes under `src/` (notebooks, pipelines, SQL objects).
3. Test in a Fabric development workspace (workspace suffix `(D)`).
4. Submit a pull request with documentation describing what changed.
5. The default branch is `main`; do not push directly to it.
