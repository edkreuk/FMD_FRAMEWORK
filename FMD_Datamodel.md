# FMD Datamodel Documentation

![FMD Metadata Overview](images/FMD_METADATA_OVERVIEW.png)

## Tables

### Integration Schema

#### Connection

Table to store all the connections
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| ConnectionId        | int                | PRIMARY KEY, IDENTITY(1,1)         |             |
| ConnectionGuid      | uniqueidentifier   | UNIQUE                             |      Guid of the connection which you find in Fabroc       |
| Name                | varchar(200)       | NOT NULL                           |       Name of the connection      |
| Type                | varchar(50)        | NOT NULL                           |             |
| GatewayType         | varchar(50)        | NULL                               |             |
| DatasourceReference | varchar(max)       | NULL                               |             |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            |             |

#### DataSource
Table to store all the datasources every Datasource has a connection to 1 Connection
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| DataSourceId        | int                | PRIMARY KEY, IDENTITY(1,1)         |             |
| ConnectionId        | int                | NOT NULL                           |             |
| Name                | varchar(100)       | NOT NULL                           |    Name of the Datasource in case of SQL server, this is the field for the databasename         |
| Namespace           | varchar(100)       | NOT NULL                           |      Prefix for the table in the lakehouse       |
| Type                | varchar(30)        | NULL                               |   Check below which type's are supported, Type are used to define the correct pipeline execution        |
| Description         | nvarchar(200)      | NULL                               |             |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            |             |

|Connection           | Type               |
|---------------------|--------------------|
|SQL Connection       |ASQL_01 ASQL_02     |
|Datalake             |ADLS_01             |
|Azure Data Factory   |ADF             |
|OneLake              |ONELAKE_TABLES_01   |         

#### Workspace
Table to store the workspaces, during the initial setup all workspaces are added by default
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| WorkspaceId         | int                | PRIMARY KEY, IDENTITY(1,1)         |             |
| WorkspaceGuid       | uniqueidentifier   | UNIQUE                             |   WorkspaceGuid from Fabric          |
| Name                | varchar(100)       | NOT NULL                           |             |

#### Pipeline
Table to store the Pipelines per workspace, during the initial setup all pipelines are added by default. Currently  the table is not used in any of the processes in the FMD_Framework
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| PipelineId          | int                | PRIMARY KEY, IDENTITY(1,1)         |             |
| PipelineGuid        | uniqueidentifier   | UNIQUE                             |             |
| WorkspaceGuid       | uniqueidentifier   | NOT NULL                           |             |
| Name                | varchar(200)       | NOT NULL                           |             |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            |             |

#### Lakehouse
Table to store the workspaces, during the initial setup all Lakehouse are added by default
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| LakehouseId         | int                | PRIMARY KEY, IDENTITY(1,1)         |             |
| LakehouseGuid       | uniqueidentifier   | UNIQUE                             |             |
| WorkspaceGuid       | uniqueidentifier   | NOT NULL                           |             |
| Name                | varchar(100)       | NOT NULL                           |             |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            |             |

#### LandingzoneEntity
Table to store the Landingzoen entitie
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| LandingzoneEntityId | bigint             | PRIMARY KEY, IDENTITY(1,1)         |             |
| DataSourceId        | int                | NOT NULL                           |   Connection to the Datasource           |
| LakehouseId         | int                | NOT NULL                           |   Connection to the Lakehouse  
LH_DATA_LANDINGZONE            |
| SourceSchema        | nvarchar(100)      | NULL                               |   Schema of the Source table. Folder of theFile          |
| SourceName          | nvarchar(200)      | NOT NULL                           |   Name of the table or File          |
| FileName            | nvarchar(200)      | NOT NULL                           |   How should the File be stored in the Landingzone advise SourceSchema_SourceTable         |
| FileType            | nvarchar(20)       | NOT NULL                           |   csv/parquet          |
| FilePath            | nvarchar(500)      | NOT NULL                           |   folder in the Lakehouse          |
| IsIncremental       | bit                | NOT NULL, DEFAULT ((0))            |       Incremental loading enabled      |
| IsIncrementalColumn | nvarchar(50)       | NULL                               |      In Incremental Loading enabled which column       |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            |             |

#### BronzeLayerEntity
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| BronzeLayerEntityId | bigint             | PRIMARY KEY, IDENTITY(1,1)         |             |
| LandingzoneEntityId | bigint             | NOT NULL                           |   Reference to the Landingzone Entity          |
| LakehouseId         | int                | NOT NULL                           |    Connection to the Lakehouse  LH_BRONZE_LAYER         |
| Schema              | nvarchar(100)      | NOT NULL                           |   Schema in the Bronze Layer, advise use the same as the Landingzone for transparancy          |Name in the Bronze Layer, advise use the same as the Landingzone for transparancy
| Name                | nvarchar(200)      | NOT NULL                           |             |
| PrimaryKeys         | nvarchar(200)      | NOT NULL                           |   What are/is the Primarykey of this table, this is needed to check if the records already exist or not          |
| FileType            | nvarchar(20)       | NOT NULL, DEFAULT ('Delta')        |   Delta          |
| CleansingRules      | nvarchar(max)      | NULL                               |    Which cleansing rules should be applied. Check in documentation how this works         |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            |             |

#### SilverLayerEntity
| Column Name         | Data Type          | Constraints                        | Description |
|---------------------|--------------------|------------------------------------|-------------|
| SilverLayerEntityId | bigint             | PRIMARY KEY, IDENTITY(1,1)         |             |
| BronzeLayerEntityId | bigint             | NOT NULL                           | Reference to the BronzeLayer Entity             |
| LakehouseId         | int                | NOT NULL                           | Connection to the Lakehouse  LH_SILVER_LAYER         |
| Schema              | nvarchar(100)      | NULL                               | Schema in the Bronze Layer, advise use the same as the Landingzone for transparancy            |
| Name                | nvarchar(200)      | NULL                               |   Table in the Bronze Layer, advise use the same as the Landingzone for transparancy          |
| FileType            | nvarchar(20)       | NOT NULL, DEFAULT ('Delta')        |    Delta         |
| CleansingRules      | nvarchar(max)      | NULL                               |  hich cleansing rules should be applied. Check in documentation how this works              |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            |             |

### Execution Schema

#### LandingzoneEntityLastLoadValue
Table to store the LastLoadValue, which is needed for Incremental Loading
| Column Name             | Data Type          | Constraints                        | Description |
|-------------------------|--------------------|------------------------------------|-------------|
| LandingzoneEntityValueId| bigint             | PRIMARY KEY, IDENTITY(1,1)         |             |
| LandingzoneEntityId     | bigint             | NULL                               |             |
| LoadValue               | varchar(50)        | NULL                               |             |
| LastLoadDatetime        | datetime2(7)       | NULL                               |             |

#### PipelineLandingzoneEntity
Table to store the processed LandingzoneEntities, so that we know which files we need to process in Bronze
| Column Name             | Data Type          | Constraints                        | Description |
|-------------------------|--------------------|------------------------------------|-------------|
| PipelineLandingzoneEntityId | bigint         | PRIMARY KEY, IDENTITY(1,1)         |             |
| LandingzoneEntityId     | bigint             | NOT NULL                           |             |
| FilePath                | nvarchar(300)      | NOT NULL                           |             |
| FileName                | nvarchar(max)      | NOT NULL                           |             |
| InsertDateTime          | datetime           | NULL                               |             |
| IsProcessed             | bit                | NOT NULL                           |             |
| LoadEndDateTime         | datetime           | NULL                               |             |

## Stored Procedures

### [execution].[sp_UpsertLandingZoneEntityLastLoadValue]

This stored procedure inserts or updates the last load value for a given LandingzoneEntityId in the `LandingzoneEntityLastLoadValue` table.

### [execution].[sp_UpsertPipelineLandingzoneEntity]

This stored procedure inserts or updates the processed status of a LandingzoneEntity in the `PipelineLandingzoneEntity` table.

### [integration].[sp_GetBronzeLayerEntity]

This stored procedure retrieves the BronzeLayerEntityId for a given LandingzoneEntityId from the `BronzeLayerEntity` table.

### [integration].[sp_GetDataSource]

This stored procedure retrieves the DataSourceId for a given Name from the `DataSource` table.

### [integration].[sp_GetLakehouse]

This stored procedure retrieves the LakehouseId for a given WorkspaceGuid and Name from the `Lakehouse` table.

### [integration].[sp_GetLandingzoneEntity]

This stored procedure retrieves the LandingzoneEntityId for a given LakehouseId, SourceSchema, and SourceName from the `LandingzoneEntity` table.

### [integration].[sp_GetSilverLayerEntity]

This stored procedure retrieves the SilverLayerEntityId for a given BronzeLayerEntityId from the `SilverLayerEntity` table.

### [integration].[sp_UpsertBronzeLayerEntity]

This stored procedure inserts or updates a BronzeLayerEntity in the `BronzeLayerEntity` table.

### [integration].[sp_UpsertConnection]

This stored procedure inserts or updates a Connection in the `Connection` table.

### [integration].[sp_UpsertDataSource]

This stored procedure inserts or updates a DataSource in the `DataSource` table.

### [integration].[sp_UpsertLakehouse]

This stored procedure inserts or updates a Lakehouse in the `Lakehouse` table.

### [integration].[sp_UpsertLandingzoneEntity]

This stored procedure inserts or updates a LandingzoneEntity in the `LandingzoneEntity` table.

### [integration].[sp_UpsertPipeline]

This stored procedure inserts or updates a Pipeline in the `Pipeline` table.

### [integration].[sp_UpsertSilverLayerEntity]

This stored procedure inserts or updates a SilverLayerEntity in the `SilverLayerEntity` table.

### [integration].[sp_UpsertWorkspace]

This stored procedure inserts or updates a Workspace in the `Workspace` table.

### [integration].[sp_GetConnection]

This stored procedure retrieves the ConnectionId for a given ConnectionGuid from the `Connection` table.

