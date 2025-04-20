# FMD Datamodel Documentation

![FMD Metadata Overview](/Images/FMD_METADATA_OVERVIEW.png)

## Tables


### Integration Schema

#### Connection

Table to store all the connections.

| Column Name         | Data Type          | Constraints                        | Description                           |
|---------------------|--------------------|------------------------------------|---------------------------------------|
| ConnectionId        | int                | PRIMARY KEY, IDENTITY(1,1)         | Unique identifier for the connection. |
| ConnectionGuid      | uniqueidentifier   | UNIQUE                             | GUID of the connection in Fabric.     |
| Name                | varchar(200)       | NOT NULL                           | Name of the connection.               |
| Type                | varchar(50)        | NOT NULL                           | Type of the connection.               |
| GatewayType         | varchar(50)        | NULL                               | Type of gateway used, if applicable.  |
| DatasourceReference | varchar(max)       | NULL                               | Reference to the data source.         |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            | Indicates if the connection is active.|

#### DataSource

Table to store all datasources. Each datasource is associated with one connection.

| Column Name         | Data Type          | Constraints                        | Description                                                                 |
|---------------------|--------------------|------------------------------------|-----------------------------------------------------------------------------|
| DataSourceId        | int                | PRIMARY KEY, IDENTITY(1,1)         | Unique identifier for the datasource.                                      |
| ConnectionId        | int                | NOT NULL                           | Reference to the associated connection.                                    |
| Name                | varchar(100)       | NOT NULL                           | Name of the datasource. For SQL Server, this represents the database name. |
| Namespace           | varchar(100)       | NOT NULL                           | Prefix for the table in the lakehouse.                                     |
| Type                | varchar(30)        | NULL                               | Defines the type of datasource, used to determine the correct pipeline execution. |
| Description         | nvarchar(200)      | NULL                               | Description of the datasource.                                             |
| IsActive            | bit                | NOT NULL, DEFAULT ((1))            | Indicates if the datasource is active.                                     |

##### Connection Types

| Connection           | Type               |
|----------------------|--------------------|
| SQL Connection       | ASQL_01, ASQL_02  |
| Datalake             | ADLS_01           |
| Azure Data Factory   | ADF               |

#### Workspace
Table to store workspaces. All workspaces are added by default during the initial setup.

| Column Name   | Data Type        | Constraints                | Description                     |
|---------------|------------------|----------------------------|---------------------------------|
| WorkspaceId   | int              | PRIMARY KEY, IDENTITY(1,1) | Unique identifier for workspace |
| WorkspaceGuid | uniqueidentifier | UNIQUE                     | Workspace GUID from Fabric      |
| Name          | varchar(100)     | NOT NULL                   | Name of the workspace           |

#### Pipeline
Table to store the pipelines per workspace. All pipelines are added by default during the initial setup. Currently, this table is not used in any processes within the FMD Framework.

| Column Name   | Data Type        | Constraints                | Description                     |
|---------------|------------------|----------------------------|---------------------------------|
| PipelineId    | int              | PRIMARY KEY, IDENTITY(1,1) | Unique identifier for pipeline  |
| PipelineGuid  | uniqueidentifier | UNIQUE                     | Pipeline GUID from Fabric       |
| WorkspaceGuid | uniqueidentifier | NOT NULL                   | Reference to the workspace      |
| Name          | varchar(200)     | NOT NULL                   | Name of the pipeline            |
| IsActive      | bit              | NOT NULL, DEFAULT ((1))    | Indicates if the pipeline is active |

#### Lakehouse
Table to store lakehouses. All lakehouses are added by default during the initial setup.

| Column Name   | Data Type        | Constraints                | Description                     |
|---------------|------------------|----------------------------|---------------------------------|
| LakehouseId   | int              | PRIMARY KEY, IDENTITY(1,1) | Unique identifier for lakehouse |
| LakehouseGuid | uniqueidentifier | UNIQUE                     | Lakehouse GUID from Fabric      |
| WorkspaceGuid | uniqueidentifier | NOT NULL                   | Reference to the workspace      |
| Name          | varchar(100)     | NOT NULL                   | Name of the lakehouse           |
| IsActive      | bit              | NOT NULL, DEFAULT ((1))    | Indicates if the lakehouse is active |

#### LandingzoneEntity
Table to store the landing zone entity.

| Column Name           | Data Type        | Constraints                | Description                                         |
|-----------------------|------------------|----------------------------|-----------------------------------------------------|
| LandingzoneEntityId   | bigint           | PRIMARY KEY, IDENTITY(1,1) | Unique identifier for the landing zone entity      |
| DataSourceId          | int              | NOT NULL                   | Reference to the data source                       |
| LakehouseId           | int              | NOT NULL                   | Reference to the lakehouse                         |
| SourceSchema          | nvarchar(100)    | NULL                       | Schema of the source table or folder of the file   |
| SourceName            | nvarchar(200)    | NOT NULL                   | Name of the source table or file                  |
| SourceCustomSelect    | nvarchar(4000)   | NULL                       | Optional custom select value                       |
| FileName              | nvarchar(200)    | NOT NULL                   | File name in the landing zone                     |
| FileType              | nvarchar(20)     | NOT NULL                   | File type (e.g., csv, parquet)                    |
| FilePath              | nvarchar(500)    | NOT NULL                   | Folder path in the lakehouse                      |
| IsIncremental         | bit              | NOT NULL, DEFAULT ((0))    | Indicates if incremental loading is enabled       |
| IsIncrementalColumn   | nvarchar(50)     | NULL                       | Column used for incremental loading               |
| IsActive              | bit              | NOT NULL, DEFAULT ((1))    | Indicates if the entity is active                 |

![LandingzoneEntity](/Images/FMD_LandingzoneEntity.png)

#### BronzeLayerEntity
Table to store entities in the bronze layer.

| Column Name         | Data Type        | Constraints                | Description                                         |
|---------------------|------------------|----------------------------|-----------------------------------------------------|
| BronzeLayerEntityId | bigint           | PRIMARY KEY, IDENTITY(1,1) | Unique identifier for the bronze layer entity      |
| LandingzoneEntityId | bigint           | NOT NULL                   | Reference to the landing zone entity               |
| LakehouseId         | int              | NOT NULL                   | Reference to the lakehouse                         |
| Schema              | nvarchar(100)    | NOT NULL                   | Schema in the bronze layer                         |
| Name                | nvarchar(200)    | NOT NULL                   | Name of the table                                  |
| PrimaryKeys         | nvarchar(200)    | NOT NULL                   | Primary keys for the table                         |
| FileType            | nvarchar(20)     | NOT NULL, DEFAULT ('Delta')| File type (e.g., Delta)                            |
| CleansingRules      | nvarchar(max)    | NULL                       | Cleansing rules to be applied                     |
| IsActive            | bit              | NOT NULL, DEFAULT ((1))    | Indicates if the entity is active                 |

![BronzeLayerEntity](/Images/FMD_BronzeLayerEntity.png)

#### SilverLayerEntity
Table to store entities in the silver layer.

| Column Name         | Data Type        | Constraints                | Description                                         |
|---------------------|------------------|----------------------------|-----------------------------------------------------|
| SilverLayerEntityId | bigint           | PRIMARY KEY, IDENTITY(1,1) | Unique identifier for the silver layer entity      |
| BronzeLayerEntityId | bigint           | NOT NULL                   | Reference to the bronze layer entity               |
| LakehouseId         | int              | NOT NULL                   | Reference to the lakehouse                         |
| Schema              | nvarchar(100)    | NULL                       | Schema in the silver layer                         |
| Name                | nvarchar(200)    | NULL                       | Name of the table                                  |
| FileType            | nvarchar(20)     | NOT NULL, DEFAULT ('Delta')| File type (e.g., Delta)                            |
| CleansingRules      | nvarchar(max)    | NULL                       | Cleansing rules to be applied                     |
| IsActive            | bit              | NOT NULL, DEFAULT ((1))    | Indicates if the entity is active                 |

![SilverLayerEntity](/Images/FMD_SilverLayerEntity.png)

