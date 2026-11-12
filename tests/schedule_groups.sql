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

ROLLBACK TRANSACTION;
PRINT 'Schedule group initialization, idempotence, registration, and view checks passed.';
