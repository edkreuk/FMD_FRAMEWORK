


    CREATE    PROCEDURE [execution].[sp_UpsertPipelineBronzeLayerEntity] (
        @BronzeLayerEntityId BIGINT,
        @SchemaName NVARCHAR(300),
        @TableName NVARCHAR(300),
        @IsProcessed BIT
    )
    WITH EXECUTE AS CALLER
    AS
    BEGIN
        SET NOCOUNT ON;

        IF NOT EXISTS (
            SELECT 1
            FROM [execution].[PipelineBronzeLayerEntity] PLE
            WHERE PLE.[BronzeLayerEntityId] = @BronzeLayerEntityId
                AND PLE.[SchemaName] = @SchemaName
                AND PLE.[TableName] = @TableName
        )
        BEGIN
            INSERT INTO [execution].[PipelineBronzeLayerEntity] (
                [BronzeLayerEntityId],
                [TableName],
                [SchemaName],
                [InsertDateTime],
                [IsProcessed]
            )
            SELECT @BronzeLayerEntityId,
                @TableName,
                @SchemaName,
                GETDATE(),
                @IsProcessed;
        END
        ELSE IF @IsProcessed = 1
        BEGIN
            UPDATE [execution].[PipelineBronzeLayerEntity]
            SET [IsProcessed] = @IsProcessed,
                [LoadEndDateTime] = GETDATE()
            WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId
                AND [SchemaName] = @SchemaName
                AND [TableName] = @TableName;
        END

        -- Output for Fabric Pipeline
        SELECT @BronzeLayerEntityId AS BronzeLayerEntityId, 
            @IsProcessed as IsProcessed,
            @TableName as TableName,
            @SchemaName as [SchemaName];

        SET NOCOUNT OFF;
    END

GO

