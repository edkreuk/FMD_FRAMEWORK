---
Title: FMD Data Model reference
Description: Learn about the metadata tables and schema used by the Fabric Metadata-Driven (FMD) Framework.
Topic: reference
Date: 07/2025
Author: edkreuk
---

# FMD Data Model reference

This article describes the metadata tables and schema used by the Fabric Metadata-Driven (FMD) Framework. The data model enables dynamic orchestration, lineage, and governance across your data platform.

![FMD Metadata Overview](/Images/FMD_METADATA_OVERVIEW.png)

## Integration schema

The integration schema contains core metadata tables for connections, data sources, workspaces, pipelines, and lakehouses.

### Connection

Stores all connection definitions.

| Column           | Data type        | Constraints                    | Description                                 |
|------------------|-----------------|--------------------------------|---------------------------------------------|
| ConnectionId     | int             | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for the connection        |
| ConnectionGuid   | uniqueidentifier| UNIQUE                         | GUID of the connection in Fabric            |
| Name             | varchar(200)    | NOT NULL                       | Name of the connection                      |
| Type             | varchar(50)     | NOT NULL                       | Type of the connection                      |
| GatewayType      | varchar(50)     | NULL                           | Type of gateway used, if applicable         |
| DatasourceReference | varchar(max) | NULL                           | Reference to the data source                |
| IsActive         | bit             | NOT NULL, DEFAULT ((1))        | Indicates if the connection is active       |

### DataSource

Stores all data sources. Each data source is associated with a connection.

| Column           | Data type        | Constraints                    | Description                                 |
|------------------|-----------------|--------------------------------|---------------------------------------------|
| DataSourceId     | int             | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for the data source       |
| ConnectionId     | int             | NOT NULL                       | Reference to the associated connection      |
| Name             | varchar(100)    | NOT NULL                       | Name of the data source (e.g., database)    |
| Namespace        | varchar(100)    | NOT NULL                       | Prefix for the table in the lakehouse       |
| Type             | varchar(30)     | NULL                           | Type of data source (for pipeline selection)|
| Description      | nvarchar(200)   | NULL                           | Description of the data source              |
| IsActive         | bit             | NOT NULL, DEFAULT ((1))        | Indicates if the data source is active      |

#### Connection types

| Connection         | Type         |
|--------------------|-------------|
| SQL Connection     | ASQL_01, ASQL_02 |
| Datalake           | ADLS_01     |
| Azure Data Factory | ADF         |

### Workspace

Stores workspace metadata. Workspaces are added by default during setup.

| Column         | Data type        | Constraints                    | Description                                 |
|----------------|-----------------|--------------------------------|---------------------------------------------|
| WorkspaceId    | int             | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for workspace             |
| WorkspaceGuid  | uniqueidentifier| UNIQUE                         | Workspace GUID from Fabric                  |
| Name           | varchar(100)    | NOT NULL                       | Name of the workspace                       |

### Pipeline

Stores pipeline metadata per workspace. Populated during setup.

| Column         | Data type        | Constraints                    | Description                                 |
|----------------|-----------------|--------------------------------|---------------------------------------------|
| PipelineId     | int             | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for pipeline              |
| PipelineGuid   | uniqueidentifier| UNIQUE                         | Pipeline GUID from Fabric                   |
| WorkspaceGuid  | uniqueidentifier| NOT NULL                       | Reference to the workspace                  |
| Name           | varchar(200)    | NOT NULL                       | Name of the pipeline                        |
| IsActive       | bit             | NOT NULL, DEFAULT ((1))        | Indicates if the pipeline is active         |

### Lakehouse

Stores lakehouse metadata. Populated during setup.

| Column         | Data type        | Constraints                    | Description                                 |
|----------------|-----------------|--------------------------------|---------------------------------------------|
| LakehouseId    | int             | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for lakehouse             |
| LakehouseGuid  | uniqueidentifier| UNIQUE                         | Lakehouse GUID from Fabric                  |
| WorkspaceGuid  | uniqueidentifier| NOT NULL                       | Reference to the workspace                  |
| Name           | varchar(100)    | NOT NULL                       | Name of the lakehouse                       |
| IsActive       | bit             | NOT NULL, DEFAULT ((1))        | Indicates if the lakehouse is active        |

## Entity tables

These tables define the entities managed in each layer of the medallion architecture.

### LandingzoneEntity

Stores metadata for landing zone entities.

| Column                | Data type        | Constraints                    | Description                                 |
|-----------------------|-----------------|--------------------------------|---------------------------------------------|
| LandingzoneEntityId   | bigint          | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for the landing zone entity|
| DataSourceId          | int             | NOT NULL                       | Reference to the data source                |
| LakehouseId           | int             | NOT NULL                       | Reference to the lakehouse                  |
| SourceSchema          | nvarchar(100)   | NULL                           | Schema of the source table or file folder   |
| SourceName            | nvarchar(200)   | NOT NULL                       | Name of the source table or file            |
| SourceCustomSelect    | nvarchar(4000)  | NULL                           | Optional custom select value                |
| CustomNotebookName    | varchar(200)    | NULL                           | Name of the custom notebook                 |
| FileName              | nvarchar(200)   | NOT NULL                       | File name in the landing zone               |
| FileType              | nvarchar(20)    | NOT NULL                       | File type (e.g., csv, parquet)              |
| FilePath              | nvarchar(500)   | NOT NULL                       | Folder path in the lakehouse                |
| IsIncremental         | bit             | NOT NULL, DEFAULT ((0))        | Indicates if incremental loading is enabled |
| IsIncrementalColumn   | nvarchar(50)    | NULL                           | Column used for incremental loading         |
| IsActive              | bit             | NOT NULL, DEFAULT ((1))        | Indicates if the entity is active           |


![LandingzoneEntity](/Images/FMD_LandingzoneEntity.png)

### BronzeLayerEntity

Stores metadata for bronze layer entities.

| Column                | Data type        | Constraints                    | Description                                 |
|-----------------------|-----------------|--------------------------------|---------------------------------------------|
| BronzeLayerEntityId   | bigint          | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for the bronze entity     |
| LandingzoneEntityId   | bigint          | NOT NULL                       | Reference to the landing zone entity        |
| LakehouseId           | int             | NOT NULL                       | Reference to the lakehouse                  |
| Schema                | nvarchar(100)   | NOT NULL                       | Schema in the bronze layer                  |
| Name                  | nvarchar(200)   | NOT NULL                       | Name of the table                           |
| PrimaryKeys           | nvarchar(200)   | NOT NULL                       | Primary keys for the table                  |
| FileType              | nvarchar(20)    | NOT NULL, DEFAULT ('Delta')    | File type (e.g., Delta)                     |
| CleansingRules        | nvarchar(max)   | NULL                           | Cleansing rules to be applied               |
| IsActive              | bit             | NOT NULL, DEFAULT ((1))        | Indicates if the entity is active           |

![BronzeLayerEntity](/Images/FMD_BronzeLayerEntity.png)

### SilverLayerEntity

Stores metadata for silver layer entities.

| Column                | Data type        | Constraints                    | Description                                 |
|-----------------------|-----------------|--------------------------------|---------------------------------------------|
| SilverLayerEntityId   | bigint          | PRIMARY KEY, IDENTITY(1,1)     | Unique identifier for the silver entity     |
| BronzeLayerEntityId   | bigint          | NOT NULL                       | Reference to the bronze layer entity        |
| LakehouseId           | int             | NOT NULL                       | Reference to the lakehouse                  |
| Schema                | nvarchar(100)   | NULL                           | Schema in the silver layer                  |
| Name                  | nvarchar(200)   | NULL                           | Name of the table                           |
| FileType              | nvarchar(20)    | NOT NULL, DEFAULT ('Delta')    | File type (e.g., Delta)                     |
| CleansingRules        | nvarchar(max)   | NULL                           | Cleansing rules to be applied               |
| IsActive              | bit             | NOT NULL, DEFAULT ((1))        | Indicates if the entity is active           |

![SilverLayerEntity](/Images/FMD_SilverLayerEntity.png)

