
CREATE PROCEDURE [integration].[sp_UpsertLandingzoneBronzeSilver]
(
    @DataSourceId INT,
    @WorkspaceGuid UNIQUEIDENTIFIER,
    @SourceSchema NVARCHAR(100),
    @SourceName NVARCHAR(200),
    @TargetSchema NVARCHAR(100),
    @TargetName NVARCHAR(200),
    @SourceCustomSelect NVARCHAR(4000),
    @FileName NVARCHAR(200),
    @FilePath NVARCHAR(100),
    @FileType NVARCHAR(20),
    @IsIncremental BIT,
    @IsIncrementalColumn NVARCHAR(50) = NULL,
    @CustomNotebookName VARCHAR(200),

    -- Bronze parameters
    @PrimaryKeys NVARCHAR(200)
)
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Output tables
        DECLARE @LandingzoneOutput TABLE (LandingzoneEntityId INT);
        DECLARE @BronzeOutput      TABLE (BronzeLayerEntityId INT);
        DECLARE @SilverOutput      TABLE (SilverLayerEntityId INT);

        DECLARE @IsActiveLandingzone BIT = 1;
        DECLARE @IsActiveBronze      BIT = 1;
        DECLARE @IsActiveSilver      BIT = 1;

        DECLARE @BronzeFileType NVARCHAR(20) = N'Delta';
        DECLARE @SilverFileType NVARCHAR(20) = N'Delta';

        -- Lookup Lakehouse IDs
        DECLARE @LakehouseId INT, @BronzeLakehouseId INT, @SilverLakehouseId INT, @LandingzoneLakehouseId INT;
        EXECUTE [integration].[sp_GetLakehouse] @WorkspaceGuid, 'LH_DATA_LANDINGZONE', @LakehouseId OUTPUT;
        SET @LandingzoneLakehouseId = @LakehouseId;

        ------------------------------------------------------------
        -- 1. Upsert Landingzone
        ------------------------------------------------------------
        MERGE [integration].[LandingzoneEntity] AS target
        USING (
            SELECT
                @SourceSchema AS SourceSchema,
                @SourceName   AS SourceName,
                @DataSourceId AS DataSourceId,
                @LandingzoneLakehouseId AS LakehouseId
        ) AS source
        ON  target.SourceSchema = source.SourceSchema
        AND target.SourceName   = source.SourceName
        AND target.DataSourceId = source.DataSourceId
        AND target.LakehouseId  = source.LakehouseId
        WHEN MATCHED THEN
            UPDATE SET
                DataSourceId        = @DataSourceId,
                IsActive            = @IsActiveLandingzone,
                SourceCustomSelect  = @SourceCustomSelect,
                FileName            = @FileName,
                FileType            = @FileType,
                FilePath            = @FilePath,
                IsIncremental       = @IsIncremental,
                IsIncrementalColumn = @IsIncrementalColumn,
                CustomNotebookName  = @CustomNotebookName
        WHEN NOT MATCHED THEN
            INSERT (
                DataSourceId, IsActive, SourceSchema, SourceName, SourceCustomSelect,
                FileName, FileType, FilePath, LakehouseId, IsIncremental, IsIncrementalColumn, CustomNotebookName
            )
            VALUES (
                @DataSourceId, @IsActiveLandingzone, @SourceSchema, @SourceName, @SourceCustomSelect,
                @FileName, @FileType, @FilePath, @LandingzoneLakehouseId, @IsIncremental, @IsIncrementalColumn, @CustomNotebookName
            )
        OUTPUT INSERTED.LandingzoneEntityId INTO @LandingzoneOutput;

        DECLARE @FinalLandingzoneId INT = (SELECT TOP (1) LandingzoneEntityId FROM @LandingzoneOutput);

        ------------------------------------------------------------
        -- 2. Upsert Bronze Layer
        ------------------------------------------------------------
        SET @LakehouseId = 0;
        EXECUTE [integration].[sp_GetLakehouse] @WorkspaceGuid, 'LH_BRONZE_LAYER', @LakehouseId OUTPUT;
        SET @BronzeLakehouseId = @LakehouseId;

        MERGE [integration].[BronzeLayerEntity] AS target
        USING (
            SELECT
                @TargetSchema AS TargetSchema,
                @TargetName   AS TargetName,
                @BronzeLakehouseId AS LakehouseId
        ) AS source
        ON  target.[Schema]    = source.TargetSchema
        AND target.[Name]      = source.TargetName
        AND target.[LakehouseId] = source.LakehouseId
        WHEN MATCHED THEN
            UPDATE SET
                [LandingzoneEntityId] = @FinalLandingzoneId,
                [IsActive]            = @IsActiveBronze,
                [Schema]              = @TargetSchema,
                [Name]                = @TargetName,
                [FileType]            = @BronzeFileType,
                [PrimaryKeys]         = @PrimaryKeys
        WHEN NOT MATCHED THEN
            INSERT ([LandingzoneEntityId], [IsActive], [Schema], [Name], [FileType], [LakehouseId], [PrimaryKeys])
            VALUES (@FinalLandingzoneId, @IsActiveBronze, @TargetSchema, @TargetName, @BronzeFileType, @BronzeLakehouseId, @PrimaryKeys)
        OUTPUT INSERTED.BronzeLayerEntityId INTO @BronzeOutput;

        DECLARE @FinalBronzeId INT = (SELECT TOP (1) BronzeLayerEntityId FROM @BronzeOutput);

        ------------------------------------------------------------
        -- 3. Upsert Silver Layer
        ------------------------------------------------------------
        SET @LakehouseId = 0;
        EXECUTE [integration].[sp_GetLakehouse] @WorkspaceGuid, 'LH_SILVER_LAYER', @LakehouseId OUTPUT;
        SET @SilverLakehouseId = @LakehouseId;

        MERGE [integration].[SilverLayerEntity] AS target
        USING (SELECT @FinalBronzeId AS BronzeLayerEntityId) AS source
        ON target.BronzeLayerEntityId = source.BronzeLayerEntityId
        WHEN MATCHED THEN
            UPDATE SET
                [BronzeLayerEntityId] = @FinalBronzeId,
                [Schema]               = @TargetSchema,
                [Name]                 = @TargetName,
                [FileType]             = @SilverFileType,
                [LakehouseId]          = @SilverLakehouseId,
                [IsActive]             = @IsActiveSilver
        WHEN NOT MATCHED THEN
            INSERT ([BronzeLayerEntityId], [IsActive], [Schema], [Name], [FileType], [LakehouseId])
            VALUES (@FinalBronzeId, @IsActiveSilver, @TargetSchema, @TargetName, @SilverFileType, @SilverLakehouseId)
        OUTPUT INSERTED.SilverLayerEntityId INTO @SilverOutput;

        DECLARE @FinalSilverId INT = (SELECT TOP (1) SilverLayerEntityId FROM @SilverOutput);

        ------------------------------------------------------------
        -- Final Output
        ------------------------------------------------------------
        SELECT
            LandingzoneEntityId = @FinalLandingzoneId,
            BronzeLayerEntityId = @FinalBronzeId,
            SilverLayerEntityId = @FinalSilverId;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT           = ERROR_SEVERITY();
        DECLARE @ErrorState INT              = ERROR_STATE();

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;

GO

