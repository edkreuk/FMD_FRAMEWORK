-- Run against an updated development configuration database; all changes roll back.
SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRANSACTION;

DECLARE @WorkspaceGuid UNIQUEIDENTIFIER = NEWID();
DECLARE @ConnectionId INT, @DataSourceId INT, @LakehouseId INT;
DECLARE @EntityId BIGINT, @InactiveEntityId BIGINT;
DECLARE @DefaultGroupId INT, @CustomGroupId INT;
DECLARE @MembershipCount BIGINT;
DECLARE @CustomGroupName NVARCHAR(100) = N'ScheduleGroupTest-' + CONVERT(NVARCHAR(36), NEWID());

INSERT INTO [integration].[Workspace] ([WorkspaceGuid], [Name])
VALUES (@WorkspaceGuid, 'Schedule group test');
INSERT INTO [integration].[Lakehouse] ([LakehouseGuid], [WorkspaceGuid], [Name])
VALUES (NEWID(), @WorkspaceGuid, 'LH_DATA_LANDINGZONE');
SET @LakehouseId = SCOPE_IDENTITY();
INSERT INTO [integration].[Connection] ([ConnectionGuid], [Name], [Type])
VALUES (NEWID(), 'Schedule group test', 'SQL');
SET @ConnectionId = SCOPE_IDENTITY();
INSERT INTO [integration].[DataSource] ([ConnectionId], [Name], [Namespace], [Type])
VALUES (@ConnectionId, 'Schedule group test', 'schedule_test', 'ASQL_01');
SET @DataSourceId = SCOPE_IDENTITY();

INSERT INTO [integration].[LandingzoneEntity]
    ([DataSourceId], [LakehouseId], [SourceSchema], [SourceName], [FileName], [FileType], [FilePath], [IsActive])
VALUES (@DataSourceId, @LakehouseId, 'dbo', 'Active', 'Active', 'parquet', 'test', 1);
SET @EntityId = SCOPE_IDENTITY();
INSERT INTO [integration].[LandingzoneEntity]
    ([DataSourceId], [LakehouseId], [SourceSchema], [SourceName], [FileName], [FileType], [FilePath], [IsActive])
VALUES (@DataSourceId, @LakehouseId, 'dbo', 'Inactive', 'Inactive', 'parquet', 'test', 0);
SET @InactiveEntityId = SCOPE_IDENTITY();

SELECT @DefaultGroupId = [ScheduleGroupId]
FROM [integration].[ScheduleGroup] WHERE [ScheduleGroupName] = N'Default';
IF @DefaultGroupId IS NULL
    THROW 51000, 'Direct insert did not create or find the default group.', 1;
IF (SELECT COUNT(*) FROM [integration].[ScheduleGroupEntity]
    WHERE [ScheduleGroupId] = @DefaultGroupId
        AND [LandingzoneEntityId] IN (@EntityId, @InactiveEntityId)) <> 2
    THROW 51000, 'Direct inserts were not immediately assigned to the default group.', 1;

-- Simulate pre-upgrade rows without default memberships for the backfill checks.
DELETE FROM [integration].[ScheduleGroupEntity]
WHERE [ScheduleGroupId] = @DefaultGroupId
    AND [LandingzoneEntityId] IN (@EntityId, @InactiveEntityId);

INSERT INTO [integration].[ScheduleGroup] ([ScheduleGroupName])
VALUES (@CustomGroupName);
SET @CustomGroupId = SCOPE_IDENTITY();
INSERT INTO [integration].[ScheduleGroupEntity] ([ScheduleGroupId], [LandingzoneEntityId])
VALUES (@CustomGroupId, @EntityId);

EXECUTE [integration].[sp_InitializeDefaultScheduleGroup];
SELECT @DefaultGroupId = [ScheduleGroupId]
FROM [integration].[ScheduleGroup] WHERE [ScheduleGroupName] = N'Default';

IF @DefaultGroupId IS NULL
    THROW 51000, 'Default group was not created.', 1;
IF EXISTS (
    SELECT 1 FROM [integration].[LandingzoneEntity] LZE
    WHERE NOT EXISTS (
        SELECT 1 FROM [integration].[ScheduleGroupEntity] SGE
        WHERE SGE.[LandingzoneEntityId] = LZE.[LandingzoneEntityId]
            AND SGE.[ScheduleGroupId] = @DefaultGroupId
    )
)
    THROW 51000, 'Backfill missed existing or inactive entities.', 1;
IF NOT EXISTS (
    SELECT 1 FROM [integration].[ScheduleGroupEntity]
    WHERE [ScheduleGroupId] = @CustomGroupId AND [LandingzoneEntityId] = @EntityId
)
    THROW 51000, 'Backfill removed custom membership.', 1;

SELECT @MembershipCount = COUNT_BIG(*) FROM [integration].[ScheduleGroupEntity];
EXECUTE [integration].[sp_InitializeDefaultScheduleGroup];
IF @MembershipCount <> (SELECT COUNT_BIG(*) FROM [integration].[ScheduleGroupEntity])
    THROW 51000, 'Repeated initialization changed membership counts.', 1;

IF (SELECT COUNT(*) FROM [execution].[vw_LoadSourceToLandingzone]
    WHERE [WorkspaceGuid] = @WorkspaceGuid AND [ScheduleGroupName] = N'Default') <> 1
    THROW 51000, 'Default view does not return exactly the active entity.', 1;
IF (SELECT COUNT(*) FROM [execution].[vw_LoadSourceToLandingzone]
    WHERE [WorkspaceGuid] = @WorkspaceGuid AND [ScheduleGroupName] = @CustomGroupName) <> 1
    THROW 51000, 'Custom view does not return exactly the assigned entity.', 1;
IF EXISTS (
    SELECT 1 FROM [execution].[vw_LoadSourceToLandingzone]
    WHERE [EntityId] = @InactiveEntityId
)
    THROW 51000, 'Inactive entity is visible in the landing-zone view.', 1;

DELETE FROM [integration].[ScheduleGroupEntity]
WHERE [LandingzoneEntityId] = @EntityId AND [ScheduleGroupId] = @DefaultGroupId;
EXECUTE [integration].[sp_UpsertLandingzoneEntity]
    @LandingzoneEntityId = @EntityId, @DataSourceId = @DataSourceId,
    @LakehouseId = @LakehouseId, @SourceSchema = 'dbo', @SourceName = 'Active',
    @SourceCustomSelect = '', @FileName = 'Active', @FilePath = 'test',
    @FileType = 'parquet', @IsIncremental = 0, @CustomNotebookName = '';
IF NOT EXISTS (
    SELECT 1 FROM [integration].[ScheduleGroupEntity]
    WHERE [LandingzoneEntityId] = @EntityId AND [ScheduleGroupId] = @DefaultGroupId
)
    THROW 51000, 'Entity registration did not restore default membership.', 1;

EXECUTE [integration].[sp_UpsertLandingzoneEntity]
    @DataSourceId = @DataSourceId, @LakehouseId = @LakehouseId,
    @SourceSchema = 'dbo', @SourceName = 'NewEntity', @SourceCustomSelect = '',
    @FileName = 'NewEntity', @FilePath = 'test', @FileType = 'parquet',
    @IsIncremental = 0, @CustomNotebookName = '';
IF NOT EXISTS (
    SELECT 1 FROM [integration].[LandingzoneEntity] LZE
    INNER JOIN [integration].[ScheduleGroupEntity] SGE
        ON SGE.[LandingzoneEntityId] = LZE.[LandingzoneEntityId]
    WHERE LZE.[DataSourceId] = @DataSourceId AND LZE.[SourceName] = 'NewEntity'
        AND SGE.[ScheduleGroupId] = @DefaultGroupId
)
    THROW 51000, 'New entity was not added to the default group.', 1;

INSERT INTO [integration].[Lakehouse] ([LakehouseGuid], [WorkspaceGuid], [Name])
VALUES (NEWID(), @WorkspaceGuid, 'LH_BRONZE_LAYER'),
       (NEWID(), @WorkspaceGuid, 'LH_SILVER_LAYER');
EXECUTE [integration].[sp_UpsertLandingzoneBronzeSilver]
    @DataSourceId = @DataSourceId, @WorkspaceGuid = @WorkspaceGuid,
    @SourceSchema = 'dbo', @SourceName = 'CombinedEntity',
    @TargetSchema = 'dbo', @TargetName = 'CombinedEntity', @SourceCustomSelect = '',
    @FileName = 'CombinedEntity', @FilePath = 'test', @FileType = 'parquet',
    @IsIncremental = 0, @CustomNotebookName = '', @PrimaryKeys = 'Id';
IF NOT EXISTS (
    SELECT 1 FROM [integration].[LandingzoneEntity] LZE
    INNER JOIN [integration].[ScheduleGroupEntity] SGE
        ON SGE.[LandingzoneEntityId] = LZE.[LandingzoneEntityId]
    WHERE LZE.[DataSourceId] = @DataSourceId AND LZE.[SourceName] = 'CombinedEntity'
        AND SGE.[ScheduleGroupId] = @DefaultGroupId
)
    THROW 51000, 'Combined registration did not add default membership.', 1;

DECLARE @BulkEntities TABLE ([LandingzoneEntityId] BIGINT);
INSERT INTO [integration].[LandingzoneEntity]
    ([DataSourceId], [LakehouseId], [SourceSchema], [SourceName], [FileName], [FileType], [FilePath], [IsActive])
OUTPUT inserted.[LandingzoneEntityId] INTO @BulkEntities
VALUES (@DataSourceId, @LakehouseId, 'dbo', 'BulkActive', 'BulkActive', 'parquet', 'test', 1),
       (@DataSourceId, @LakehouseId, 'dbo', 'BulkInactive', 'BulkInactive', 'parquet', 'test', 0);
IF (SELECT COUNT(*) FROM [integration].[ScheduleGroupEntity] SGE
    INNER JOIN @BulkEntities B ON B.[LandingzoneEntityId] = SGE.[LandingzoneEntityId]
    WHERE SGE.[ScheduleGroupId] = @DefaultGroupId) <> 2
    THROW 51000, 'Multi-row insert did not assign every entity to the default group.', 1;

SAVE TRANSACTION InsertRollback;
DECLARE @RolledBackEntityId BIGINT;
INSERT INTO [integration].[LandingzoneEntity]
    ([DataSourceId], [LakehouseId], [SourceSchema], [SourceName], [FileName], [FileType], [FilePath])
VALUES (@DataSourceId, @LakehouseId, 'dbo', 'RolledBack', 'RolledBack', 'parquet', 'test');
SET @RolledBackEntityId = SCOPE_IDENTITY();
ROLLBACK TRANSACTION InsertRollback;
IF EXISTS (
    SELECT 1 FROM [integration].[ScheduleGroupEntity]
    WHERE [LandingzoneEntityId] = @RolledBackEntityId
)
    THROW 51000, 'Default membership was not rolled back with its entity insert.', 1;

DECLARE @UpsertGroupName NVARCHAR(100) = N'UpsertTest-' + CONVERT(NVARCHAR(36), NEWID());
DECLARE @RenamedGroupName NVARCHAR(100) = N'Renamed-' + CONVERT(NVARCHAR(36), NEWID());
DECLARE @UpsertGroupId INT;
DECLARE @UpsertResult TABLE ([ScheduleGroupId] INT, [ScheduleGroupName] NVARCHAR(100));
INSERT INTO @UpsertResult
EXECUTE [integration].[sp_UpsertScheduleGroup] @ScheduleGroupName = @UpsertGroupName;
SELECT @UpsertGroupId = [ScheduleGroupId] FROM @UpsertResult;
IF @UpsertGroupId IS NULL
    OR NOT EXISTS (SELECT 1 FROM [integration].[ScheduleGroup]
                   WHERE [ScheduleGroupId] = @UpsertGroupId AND [ScheduleGroupName] = @UpsertGroupName)
    THROW 51000, 'Schedule group upsert did not create the group.', 1;
DELETE FROM @UpsertResult;
INSERT INTO @UpsertResult
EXECUTE [integration].[sp_UpsertScheduleGroup] @ScheduleGroupName = @UpsertGroupName;
IF (SELECT [ScheduleGroupId] FROM @UpsertResult) <> @UpsertGroupId
    OR (SELECT COUNT(*) FROM [integration].[ScheduleGroup] WHERE [ScheduleGroupName] = @UpsertGroupName) <> 1
    THROW 51000, 'Repeated schedule group upsert was not idempotent.', 1;
DELETE FROM @UpsertResult;
INSERT INTO @UpsertResult
EXECUTE [integration].[sp_UpsertScheduleGroup]
    @ScheduleGroupId = @UpsertGroupId, @ScheduleGroupName = @RenamedGroupName;
IF NOT EXISTS (SELECT 1 FROM [integration].[ScheduleGroup]
               WHERE [ScheduleGroupId] = @UpsertGroupId AND [ScheduleGroupName] = @RenamedGroupName)
    THROW 51000, 'Schedule group upsert did not rename the group by id.', 1;

DECLARE @MembershipResult TABLE ([ScheduleGroupId] INT, [LandingzoneEntityId] BIGINT);
INSERT INTO @MembershipResult
EXECUTE [integration].[sp_UpsertScheduleGroupEntity]
    @LandingzoneEntityId = @EntityId, @ScheduleGroupName = @RenamedGroupName;
INSERT INTO @MembershipResult
EXECUTE [integration].[sp_UpsertScheduleGroupEntity]
    @LandingzoneEntityId = @EntityId, @ScheduleGroupName = @RenamedGroupName;
IF (SELECT COUNT(*) FROM [integration].[ScheduleGroupEntity]
    WHERE [ScheduleGroupId] = @UpsertGroupId AND [LandingzoneEntityId] = @EntityId) <> 1
    OR (SELECT COUNT(*) FROM @MembershipResult
        WHERE [ScheduleGroupId] = @UpsertGroupId AND [LandingzoneEntityId] = @EntityId) <> 2
    THROW 51000, 'Schedule group entity upsert was not idempotent.', 1;
IF NOT EXISTS (SELECT 1 FROM [execution].[vw_LoadSourceToLandingzone]
               WHERE [EntityId] = @EntityId AND [ScheduleGroupName] = @RenamedGroupName)
    THROW 51000, 'Upserted membership is not visible in the landing-zone view.', 1;

ROLLBACK TRANSACTION;

-- Validation errors are raised before any write; run them outside a transaction.
DECLARE @ExpectedErrors INT = 0;
BEGIN TRY
    EXECUTE [integration].[sp_UpsertScheduleGroup] @ScheduleGroupName = N'  ';
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() = 50002 SET @ExpectedErrors += 1;
END CATCH;
BEGIN TRY
    EXECUTE [integration].[sp_UpsertScheduleGroup] @ScheduleGroupId = -1, @ScheduleGroupName = N'Missing';
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() = 50003 SET @ExpectedErrors += 1;
END CATCH;
BEGIN TRY
    DECLARE @DefaultId INT = (SELECT [ScheduleGroupId] FROM [integration].[ScheduleGroup]
                              WHERE [ScheduleGroupName] = N'Default');
    EXECUTE [integration].[sp_UpsertScheduleGroup] @ScheduleGroupId = @DefaultId, @ScheduleGroupName = N'NotDefault';
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() = 50004 SET @ExpectedErrors += 1;
END CATCH;
BEGIN TRY
    EXECUTE [integration].[sp_UpsertScheduleGroupEntity] @LandingzoneEntityId = -1, @ScheduleGroupName = N'Default';
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() = 50001 SET @ExpectedErrors += 1;
END CATCH;
BEGIN TRY
    DECLARE @AnyEntityId BIGINT = (SELECT TOP (1) [LandingzoneEntityId] FROM [integration].[LandingzoneEntity]);
    EXECUTE [integration].[sp_UpsertScheduleGroupEntity]
        @LandingzoneEntityId = @AnyEntityId, @ScheduleGroupName = N'MissingGroup-0f1b7c2e';
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() IN (50001, 50003) SET @ExpectedErrors += 1;
END CATCH;
IF @ExpectedErrors <> 5
    THROW 51000, 'Schedule group upsert validation did not raise the expected errors.', 1;

PRINT 'Schedule group initialization, registration, direct/bulk inserts, upserts, rollback, and view checks passed.';
