


    CREATE    PROCEDURE [execution].[sp_UpsertPipelineLandingzoneEntity] (
        @LandingzoneEntityId BIGINT,
        @Filename NVARCHAR(300),
        @FilePath NVARCHAR(300),
        @IsProcessed BIT
    )
    WITH EXECUTE AS CALLER
    AS
    BEGIN
        SET NOCOUNT ON;

        IF NOT EXISTS (
            SELECT 1
            FROM [execution].[PipelineLandingzoneEntity] PLE
            WHERE PLE.[LandingzoneEntityId] = @LandingzoneEntityId
                AND PLE.[Filename] = @Filename
                AND PLE.[FilePath] = @FilePath
        )
        BEGIN
            INSERT INTO [execution].[PipelineLandingzoneEntity] (
                [LandingzoneEntityId],
                [FilePath],
                [FileName],
                [InsertDateTime],
                [IsProcessed]
            )
            SELECT @LandingzoneEntityId,
                @FilePath,
                @Filename,
                GETDATE(),
                @IsProcessed;
        END
        ELSE IF @IsProcessed = 1
        BEGIN
            UPDATE [execution].[PipelineLandingzoneEntity]
            SET [IsProcessed] = @IsProcessed,
                [LoadEndDateTime] = GETDATE()
            WHERE [LandingzoneEntityId] = @LandingzoneEntityId
                AND [Filename] = @Filename
                AND [FilePath] = @FilePath;
        END

        -- Output for Fabric Pipeline
        SELECT @LandingzoneEntityId AS LandingzoneEntityId, 
            @IsProcessed as IsProcessed,
            @FilePath as FilePath,
            @Filename as [Filename];

        SET NOCOUNT OFF;
    END

GO

