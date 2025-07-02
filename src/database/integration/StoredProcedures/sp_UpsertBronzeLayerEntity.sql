



    CREATE    PROCEDURE [integration].[sp_UpsertBronzeLayerEntity](
        @BronzeLayerEntityId INT = 0
        ,@LandingzoneEntityId INT
        ,@Schema NVARCHAR(100) 
        ,@Name NVARCHAR(200)
        ,@FileType NVARCHAR(20) = 'Delta'
        ,@LakehouseId INT
        ,@PrimaryKeys NVARCHAR(200)
        ,@IsActive BIT = 1)
    WITH EXECUTE AS CALLER
    AS 

    BEGIN
    SET NOCOUNT ON;

    DECLARE @OutputTable TABLE (BronzeLayerEntityId INT);

    IF NOT EXISTS(SELECT 1
                  FROM [integration].[BronzeLayerEntity]
                  WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId)
    BEGIN
        INSERT INTO [integration].[BronzeLayerEntity]
            ([LandingzoneEntityId]
            ,[IsActive]
            ,[Schema]
            ,[Name]
            ,[FileType]
            ,[LakehouseId]
            ,[PrimaryKeys])
        OUTPUT INSERTED.[BronzeLayerEntityId] INTO @OutputTable
        VALUES (@LandingzoneEntityId
            ,@IsActive
            ,@Schema
            ,@Name
            ,@FileType
            ,@LakehouseId
            ,@PrimaryKeys);
    END
    ELSE
    BEGIN
        UPDATE [integration].[BronzeLayerEntity]
        SET [LandingzoneEntityId] = @LandingzoneEntityId
            ,[IsActive] = @IsActive
            ,[Schema] = @Schema
            ,[Name] = @Name
            ,[FileType] = @FileType
            ,[LakehouseId] = @LakehouseId
            ,[PrimaryKeys] = @PrimaryKeys
        OUTPUT INSERTED.[BronzeLayerEntityId] INTO @OutputTable
        WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId;
    END

    SELECT BronzeLayerEntityId FROM @OutputTable;
END

GO

