# FMD Framework - Naming Glossary

> Quick-reference guide mapping technical framework names to plain-English descriptions.
> Use this when presenting to stakeholders or onboarding new team members.

---

## Workspaces

| Technical Name | What It Is |
|---|---|
| **FMD-DEV-DATA** | Development workspace for all data storage (lakehouses) |
| **FMD-DEV-CODE** | Development workspace for all code assets (pipelines, notebooks) |
| **FMD-DEV-CONFIG** | Development workspace for configuration (SQL database, variables) |
| **FMD-PROD-DATA** | Production workspace for data storage |
| **FMD-PROD-CODE** | Production workspace for code assets |
| **FMD-PROD-CONFIG** | Production workspace for configuration |

---

## Data Storage (Lakehouses)

| Technical Name | What It Is | Layer |
|---|---|---|
| **LH_DATA_LANDINGZONE** | Raw data landing area — where source data first arrives | Landing Zone |
| **LH_BRONZE_LAYER** | Structured raw data — cleaned file formats, no transformations | Bronze |
| **LH_SILVER_LAYER** | Transformed & cleansed data — business-ready, quality-checked | Silver |

---

## Configuration Database

| Technical Name | What It Is |
|---|---|
| **SQL_FMD_FRAMEWORK** | The central metadata database that drives all pipeline behavior. Contains connection details, data source definitions, entity mappings, and processing rules. |

---

## Orchestration Pipelines (Top-Level)

| Technical Name | What It Does |
|---|---|
| **PL_FMD_LOAD_ALL** | Master orchestrator — runs the entire pipeline chain end-to-end (Landing Zone + Bronze + Silver) |
| **PL_FMD_LOAD_LANDINGZONE** | Orchestrates all Landing Zone ingestion — reads metadata DB, determines which source connector to call |
| **PL_FMD_LOAD_BRONZE** | Orchestrates Bronze layer processing — converts raw files to Delta tables |
| **PL_FMD_LOAD_SILVER** | Orchestrates Silver layer processing — applies transformations and DQ rules |

---

## Landing Zone Copy Pipelines (Source Connectors)

These are the "worker" pipelines that actually move data from each source type into the Landing Zone lakehouse.

| Technical Name | Source Type | What It Does |
|---|---|---|
| **PL_FMD_LDZ_COPY_FROM_ASQL_01** | Azure SQL / SQL Server | Copies tables from SQL databases into Landing Zone |
| **PL_FMD_LDZ_COPY_FROM_ORACLE_01** | Oracle Database | Copies tables from Oracle into Landing Zone |
| **PL_FMD_LDZ_COPY_FROM_ADLS_01** | Azure Data Lake Storage | Copies files from ADLS Gen2 into Landing Zone |
| **PL_FMD_LDZ_COPY_FROM_FTP_01** | FTP Server | Copies files from FTP servers into Landing Zone |
| **PL_FMD_LDZ_COPY_FROM_SFTP_01** | SFTP Server | Copies files from SFTP servers into Landing Zone |
| **PL_FMD_LDZ_COPY_FROM_ONELAKE_FILES_01** | OneLake (Files) | Copies files from other OneLake file locations |
| **PL_FMD_LDZ_COPY_FROM_ONELAKE_TABLES_01** | OneLake (Tables) | Copies Delta tables from other OneLake locations |
| **PL_FMD_LDZ_COPY_FROM_ADF** | Azure Data Factory | Triggers an ADF pipeline and pulls its output |
| **PL_FMD_LDZ_COPY_FROM_CUSTOM_NB** | Custom Notebook | Runs a user-defined notebook to bring in data |

---

## Landing Zone Command Pipelines (Orchestration Helpers)

These pipelines are called by `PL_FMD_LOAD_LANDINGZONE` to route each entity to the correct copy pipeline based on its source type.

| Technical Name | Routes To |
|---|---|
| **PL_FMD_LDZ_COMMAND_ASQL** | Routes SQL Server entities to the ASQL copy pipeline |
| **PL_FMD_LDZ_COMMAND_ORACLE** | Routes Oracle entities to the Oracle copy pipeline |
| **PL_FMD_LDZ_COMMAND_ADLS** | Routes ADLS entities to the ADLS copy pipeline |
| **PL_FMD_LDZ_COMMAND_FTP** | Routes FTP entities to the FTP copy pipeline |
| **PL_FMD_LDZ_COMMAND_SFTP** | Routes SFTP entities to the SFTP copy pipeline |
| **PL_FMD_LDZ_COMMAND_ONELAKE** | Routes OneLake entities to the OneLake copy pipeline |
| **PL_FMD_LDZ_COMMAND_ADF** | Routes ADF entities to the ADF copy pipeline |
| **PL_FMD_LDZ_COMMAND_NOTEBOOK** | Routes Custom NB entities to the notebook pipeline |

---

## Tooling Pipelines

| Technical Name | What It Does |
|---|---|
| **PL_TOOLING_POST_ASQL_TO_FMD** | Utility pipeline that pushes SQL query results into the FMD config database |

---

## Notebooks

| Technical Name | What It Does |
|---|---|
| **NB_FMD_UTILITY_FUNCTIONS** | Shared helper functions used by all other notebooks (connection management, Spark utilities, etc.) |
| **NB_FMD_LOAD_LANDING_BRONZE** | Processes files from Landing Zone into Bronze Delta tables |
| **NB_FMD_LOAD_BRONZE_SILVER** | Transforms Bronze data into Silver with business rules and DQ checks |
| **NB_FMD_PROCESSING_PARALLEL_MAIN** | Parallel processing engine — runs multiple entities concurrently |
| **NB_FMD_PROCESSING_LANDINGZONE_MAIN** | Landing Zone batch processor — coordinates multi-entity ingestion |
| **NB_FMD_DQ_CLEANSING** | Data Quality engine — applies validation rules, flags bad records |
| **NB_FMD_CUSTOM_NOTEBOOK_TEMPLATE** | Starter template for custom ingestion notebooks |

---

## Variable Libraries

| Technical Name | What It Stores |
|---|---|
| **VAR_FMD** | Runtime secrets — Key Vault URI, tenant ID, client credentials, lakehouse schema flag |
| **VAR_CONFIG_FMD** | Framework config — database connection string, DB name, workspace/database GUIDs |

---

## Environment

| Technical Name | What It Is |
|---|---|
| **ENV_FMD** | Spark environment definition — Python/Spark versions, library dependencies |

---

## Connections

| Technical Name | What It Connects To |
|---|---|
| **CON_FMD_FABRIC_SQL** | Connection to the SQL_FMD_FRAMEWORK metadata database |
| **CON_FMD_FABRIC_PIPELINES** | Connection used by pipelines to invoke other pipelines |
| **CON_FMD_FABRIC_NOTEBOOKS** | Connection used by pipelines to invoke notebooks |
| **CON_FMD_ADF_PIPELINES** | Connection to Azure Data Factory for ADF-based ingestion |

---

## How It All Fits Together

```
PL_FMD_LOAD_ALL (Master Orchestrator)
  |
  +-- PL_FMD_LOAD_LANDINGZONE (reads metadata, routes to correct source connector)
  |     |
  |     +-- PL_FMD_LDZ_COMMAND_ASQL --> PL_FMD_LDZ_COPY_FROM_ASQL_01
  |     +-- PL_FMD_LDZ_COMMAND_ORACLE --> PL_FMD_LDZ_COPY_FROM_ORACLE_01
  |     +-- PL_FMD_LDZ_COMMAND_ADLS --> PL_FMD_LDZ_COPY_FROM_ADLS_01
  |     +-- ... (one command + copy pipeline per source type)
  |
  +-- PL_FMD_LOAD_BRONZE (Landing Zone files --> Bronze Delta tables)
  |     |
  |     +-- NB_FMD_LOAD_LANDING_BRONZE
  |     +-- NB_FMD_PROCESSING_PARALLEL_MAIN (parallel entity processing)
  |
  +-- PL_FMD_LOAD_SILVER (Bronze --> Silver with transformations + DQ)
        |
        +-- NB_FMD_LOAD_BRONZE_SILVER
        +-- NB_FMD_DQ_CLEANSING
        +-- NB_FMD_PROCESSING_PARALLEL_MAIN
```

---

## Naming Convention Key

| Prefix | Meaning |
|---|---|
| `PL_` | Data Pipeline |
| `NB_` | Notebook |
| `LH_` | Lakehouse |
| `VAR_` | Variable Library |
| `ENV_` | Environment |
| `CON_` | Connection |
| `SQL_` | SQL Database |
| `FMD` | Fabric Metadata-Driven (framework identifier) |
| `LDZ` | Landing Zone |
| `DQ` | Data Quality |

---

*Generated from FMD Framework deployment manifests. All names are functional references — do not rename without updating all pipeline and notebook configurations.*
