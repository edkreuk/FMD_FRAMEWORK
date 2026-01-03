



    CREATE   PROCEDURE [integration].[sp_UpsertSilverLayerEntity](
         @SilverLayerEntityId INT = 0 
        ,@BronzeLayerEntityId INT
        ,@LakehouseId INT
        ,@Name NVARCHAR(200)
        ,@Schema NVARCHAR(100)
        ,@FileType NVARCHAR(20) = 'Delta'
        ,@IsActive BIT = 1)

WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OutputTable TABLE (SilverLayerEntityId INT);

    IF NOT EXISTS(SELECT 1
                  FROM [integration].[SilverLayerEntity]
                  WHERE [SilverLayerEntityId] = @SilverLayerEntityId)
    BEGIN
        INSERT INTO [integration].[SilverLayerEntity]
            ([BronzeLayerEntityId]
            ,[IsActive]
            ,[Schema]
            ,[Name]
            ,[FileType]
            ,[LakehouseId])
        OUTPUT INSERTED.[SilverLayerEntityId] INTO @OutputTable
        VALUES (@BronzeLayerEntityId
            ,@IsActive
            ,@Schema
            ,@Name
            ,@FileType
            ,@LakehouseId);
    END
    ELSE
    BEGIN
        UPDATE [integration].[SilverLayerEntity]
        SET [BronzeLayerEntityId] = @BronzeLayerEntityId
            ,[Schema] = @Schema
            ,[Name] = @Name
            ,[FileType] = @FileType
            ,[LakehouseId] = @LakehouseId
            ,[IsActive] = @IsActive
        OUTPUT INSERTED.[SilverLayerEntityId] INTO @OutputTable
        WHERE [SilverLayerEntityId] = @SilverLayerEntityId;
    END

    SELECT SilverLayerEntityId FROM @OutputTable;
END

GO

