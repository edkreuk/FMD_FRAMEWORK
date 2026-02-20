-- ============================================================================
-- MT-15/MT-16: Register Source System Connections & Entities in FMD Framework
-- ============================================================================
-- Run this script against SQL_FMD_FRAMEWORK in the Fabric portal SQL editor
-- (INTEGRATION CONFIG workspace > SQL_FMD_FRAMEWORK > New Query)
--
-- This script does 3 things:
--   STEP 1: Register gateway connections (the "door" to each SQL Server)
--   STEP 2: Register data sources (which database behind each door)
--   STEP 3: Register entities (which tables to pull from each database)
--
-- After running, trigger PL_FMD_LOAD_LANDINGZONE to test end-to-end.
-- ============================================================================


-- ============================================================================
-- STEP 0: Check what's already registered (run this first to see baseline)
-- ============================================================================
SELECT 'Existing Connections' AS Section, ConnectionId, ConnectionGuid, Name, Type, IsActive
FROM integration.Connection ORDER BY ConnectionId;

SELECT 'Existing DataSources' AS Section, ds.DataSourceId, ds.Name, ds.Namespace, ds.Type, ds.IsActive, c.Name AS ConnectionName
FROM integration.DataSource ds
JOIN integration.Connection c ON ds.ConnectionId = c.ConnectionId ORDER BY ds.DataSourceId;

SELECT 'Existing Lakehouses' AS Section, LakehouseId, Name FROM integration.Lakehouse ORDER BY LakehouseId;

SELECT 'Existing Workspaces' AS Section, WorkspaceId, Name FROM integration.Workspace ORDER BY WorkspaceId;
GO


-- ============================================================================
-- STEP 1: Register Connections
-- ============================================================================
-- ConnectionGuid = the Connection ID from Fabric Portal > Settings >
--   Manage connections and gateways > click connection > Connection ID field
-- Type = 'SqlServer' for on-prem SQL via gateway

-- Connection: M3 Cloud (sql2016live / DI_PRD_Staging)
-- Gateway: PowerBIGateway | Auth: Basic / UsrSQLRead
EXEC [integration].[sp_UpsertConnection]
    @ConnectionGuid = '1187a5d7-5d6e-4a23-8c54-2a0734350629',
    @Name = 'CON_FMD_SQL2016LIVE_M3CLOUD',
    @Type = 'SqlServer',
    @IsActive = 1;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ ADD MORE CONNECTIONS HERE as you get Connection IDs from Fabric portal │
-- │                                                                        │
-- │ Example for IPC_PowerData:                                             │
-- │ EXEC [integration].[sp_UpsertConnection]                               │
-- │     @ConnectionGuid = '<GUID from portal>',                            │
-- │     @Name = 'CON_FMD_SQL2019DEV_POWERDATA',                            │
-- │     @Type = 'SqlServer',                                               │
-- │     @IsActive = 1;                                                     │
-- │                                                                        │
-- │ Example for M3FDBTST:                                                  │
-- │ EXEC [integration].[sp_UpsertConnection]                               │
-- │     @ConnectionGuid = '<GUID from portal>',                            │
-- │     @Name = 'CON_FMD_M3DEV_M3FDBTST',                                 │
-- │     @Type = 'SqlServer',                                               │
-- │     @IsActive = 1;                                                     │
-- │                                                                        │
-- │ Example for M3TST-ETL:                                                 │
-- │ EXEC [integration].[sp_UpsertConnection]                               │
-- │     @ConnectionGuid = '<GUID from portal>',                            │
-- │     @Name = 'CON_FMD_SQL2016DEV_M3TSTETL',                             │
-- │     @Type = 'SqlServer',                                               │
-- │     @IsActive = 1;                                                     │
-- │                                                                        │
-- │ Example for MES:                                                       │
-- │ EXEC [integration].[sp_UpsertConnection]                               │
-- │     @ConnectionGuid = '<GUID from portal>',                            │
-- │     @Name = 'CON_FMD_SQL2012TEST_MES',                                 │
-- │     @Type = 'SqlServer',                                               │
-- │     @IsActive = 1;                                                     │
-- └─────────────────────────────────────────────────────────────────────────┘
GO


-- ============================================================================
-- STEP 2: Register Data Sources
-- ============================================================================
-- Links a Connection to a specific database.
-- @Name = the actual database name on the source server
--         (this becomes @item().DatasourceName in the pipeline)
-- @Type = 'ASQL_01' routes to pipeline PL_FMD_LDZ_COPY_FROM_ASQL_01
-- @Namespace = logical grouping label (your choice, used for organization)

-- Data Source: DI_PRD_Staging on sql2016live (via M3 Cloud connection)
DECLARE @DS_M3Cloud_ConnId INT = (SELECT ConnectionId FROM integration.Connection WHERE Name = 'CON_FMD_SQL2016LIVE_M3CLOUD')
DECLARE @DS_M3Cloud_DSId INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = 'DI_PRD_Staging' AND Type = 'ASQL_01')
EXEC [integration].[sp_UpsertDataSource]
    @ConnectionId = @DS_M3Cloud_ConnId,
    @DataSourceId = @DS_M3Cloud_DSId,
    @Name = 'DI_PRD_Staging',
    @Namespace = 'M3CLOUD',
    @Type = 'ASQL_01',
    @Description = 'M3 Cloud DI_PRD_Staging database on sql2016live via PowerBIGateway',
    @IsActive = 1;
GO


-- ============================================================================
-- STEP 3: Register Landing Zone Entities (tables to ingest)
-- ============================================================================
-- Each entity = one source table to pull into the Landing Zone lakehouse.
-- The pipeline will loop through all active entities for this data source
-- and copy each one as a parquet file into LH_DATA_LANDINGZONE.
--
-- @SourceSchema     = schema on source SQL Server (e.g., 'dbo')
-- @SourceName       = table/view name on source SQL Server
-- @FileName         = output file name in lakehouse (no extension)
-- @FilePath         = folder path in lakehouse Files area
-- @FileType         = 'parquet' (landing zone format)
-- @IsIncremental    = 0 for full load, 1 for incremental (needs IsIncrementalColumn)
-- @IsIncrementalColumn = column name for watermark (e.g., 'ModifiedDate')
-- @CustomNotebookName  = leave empty unless using a custom notebook for this entity
--
-- IMPORTANT: You need to know the table names in DI_PRD_Staging.
-- Run this query on the source to find tables:
--   SELECT TABLE_SCHEMA, TABLE_NAME FROM DI_PRD_Staging.INFORMATION_SCHEMA.TABLES
--   WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME
-- ============================================================================

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ TEST ENTITY: Replace 'YourTestTable' with an actual table from         │
-- │ DI_PRD_Staging. Pick a small table first to validate the pipeline.     │
-- └─────────────────────────────────────────────────────────────────────────┘

/*  -- UNCOMMENT AND EDIT when you know a table name from DI_PRD_Staging:

DECLARE @TestEntity_DSId INT = (SELECT DataSourceId FROM integration.DataSource WHERE Name = 'DI_PRD_Staging' AND Type = 'ASQL_01')
DECLARE @TestEntity_LHId INT = (SELECT TOP 1 LakehouseId FROM integration.Lakehouse WHERE Name = 'LH_DATA_LANDINGZONE')
DECLARE @TestEntity_LEId INT = (SELECT LandingzoneEntityId FROM integration.LandingzoneEntity WHERE SourceSchema = 'dbo' AND SourceName = 'YourTestTable')
EXEC [integration].[sp_UpsertLandingzoneEntity]
    @LandingzoneEntityId = @TestEntity_LEId,
    @DataSourceId = @TestEntity_DSId,
    @LakehouseId = @TestEntity_LHId,
    @SourceSchema = 'dbo',                 -- change to actual schema
    @SourceName = 'YourTestTable',         -- change to actual table name
    @SourceCustomSelect = '',              -- leave empty for SELECT *
    @FileName = 'YourTestTable',           -- output file name (no extension)
    @FilePath = 'm3cloud',                 -- folder in lakehouse
    @FileType = 'parquet',
    @IsIncremental = 0,                    -- 0 = full load
    @IsIncrementalColumn = '',
    @IsActive = 1,
    @CustomNotebookName = '';

*/


-- ============================================================================
-- VERIFICATION: Confirm everything registered correctly
-- ============================================================================
SELECT 'Connections' AS Section, ConnectionId, ConnectionGuid, Name, Type, IsActive
FROM integration.Connection ORDER BY ConnectionId;

SELECT 'DataSources' AS Section, ds.DataSourceId, ds.Name, ds.Namespace, ds.Type, ds.Description, ds.IsActive, c.Name AS ConnectionName
FROM integration.DataSource ds
JOIN integration.Connection c ON ds.ConnectionId = c.ConnectionId ORDER BY ds.DataSourceId;

SELECT 'LandingzoneEntities' AS Section, le.LandingzoneEntityId, le.SourceSchema, le.SourceName, le.FileName, le.FilePath, le.FileType, le.IsIncremental, le.IsActive, ds.Name AS DataSourceName
FROM integration.LandingzoneEntity le
JOIN integration.DataSource ds ON le.DataSourceId = ds.DataSourceId ORDER BY le.LandingzoneEntityId;

-- Check the view that pipelines actually use:
SELECT 'PipelineView' AS Section, * FROM [execution].[vw_LoadSourceToLandingzone];
