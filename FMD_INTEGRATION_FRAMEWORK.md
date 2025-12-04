---
Title: Data Integration examples for the FMD Framework
Description: Learn how to add sources using metadata-driven  for the Fabric Metadata-Driven (FMD) Framework.
Topic: how-to
Date: 12/2025
Author: edkreuk
---
# Data Integration examples for the FMD Framework

## Custom Notebooks

The Fabric Metadata-Driven (FMD) Framework enables you to define custom data integration logic using Notebooks. Create Notebooks that perform transformations, aggregations, or data manipulations within your data pipelines.

The deployment process includes a template Notebook called **NB_FMD_CUSTOM_NOTEBOOK_TEMPLATE**. Use this template as a starting point to implement your custom data integration logic.

## Configure the Notebook in Fabric Database

Before you begin, ensure you've created a connection in the FMD Framework.

### Add connection metadata

In the **integration.Connection** table, add:

| Column | Value |
|--------|-------|
| `ConnectionGuid` | `00000000-0000-0000-0000-000000000001` |
| `Name` | `CON_FMD_NOTEBOOK` |
| `Type` | `NOTEBOOK` |

### Add datasource metadata

In the **integration.Datasource** table, add:

| Column | Value |
|--------|-------|
| `ConnectionId` | Connection ID from above |
| `Name` | `CUSTOM_NOTEBOOK` |
| `Namespace` | `NB` |
| `Type` | `NOTEBOOK` |
| `Description` | `Custom Notebook` |

> [!NOTE]
> These settings are typically created during deployment.

## Register the Notebook

Add your custom Notebook to the FMD Framework by inserting a record into **integration.LandingzoneEntity**, or use the stored procedure below to automatically create related Bronze and Silver entity metadata:

```sql
DECLARE @RC int
DECLARE @DataSourceId int
DECLARE @WorkspaceGuid uniqueidentifier
DECLARE @SourceSchema nvarchar(100)
DECLARE @SourceName nvarchar(200)
DECLARE @SourceCustomSelect nvarchar(4000)
DECLARE @FileName nvarchar(200)
DECLARE @FilePath nvarchar(100)
DECLARE @FileType nvarchar(20)
DECLARE @IsIncremental bit
DECLARE @IsIncrementalColumn nvarchar(50)
DECLARE @CustomNotebookName varchar(200)
DECLARE @PrimaryKeys nvarchar(200)

EXECUTE @RC = [integration].[sp_UpsertLandingzoneBronzeSilver] 
    @DataSourceId
  ,@WorkspaceGuid
  ,@SourceSchema
  ,@SourceName
  ,@SourceCustomSelect
  ,@FileName
  ,@FilePath
  ,@FileType
  ,@IsIncremental
  ,@IsIncrementalColumn
  ,@CustomNotebookName
  ,@PrimaryKeys

GO
```

More to come in future releases!