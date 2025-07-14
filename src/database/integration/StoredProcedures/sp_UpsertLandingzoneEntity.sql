



    CREATE    PROCEDURE [integration].[sp_UpsertLandingzoneEntity](
        @LandingzoneEntityId INT = 0
        ,@DataSourceId INT
        ,@LakehouseId INT
        ,@SourceSchema NVARCHAR(100)
        ,@SourceName NVARCHAR(200)
		,@SourceCustomSelect NVARCHAR(4000)
        ,@FileName NVARCHAR(200)
        ,@FilePath NVARCHAR(100)
        ,@FileType NVARCHAR(20)
        ,@IsIncremental bit
        ,@IsIncrementalColumn NVARCHAR(50) = NULL
        ,@IsActive BIT = 1
    )
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OutputTable TABLE (LandingzoneEntityId INT);

    IF NOT EXISTS(SELECT 1
                  FROM [integration].[LandingzoneEntity]
                  WHERE [LandingzoneEntityId] = @LandingzoneEntityId)
    BEGIN
        INSERT INTO [integration].[LandingzoneEntity]
            ([DataSourceId]
            ,[IsActive]
            ,[SourceSchema]
            ,[SourceName]
			,[SourceCustomSelect]
            ,[FileName]
            ,[FileType]
            ,[FilePath]
            ,[LakehouseId]
            ,[IsIncremental]
            ,[IsIncrementalColumn])
        OUTPUT INSERTED.[LandingzoneEntityId] INTO @OutputTable
        VALUES (@DataSourceId
            ,@IsActive
            ,@SourceSchema
            ,@SourceName
			,@SourceCustomSelect
            ,@FileName
            ,@FileType
            ,@FilePath
            ,@LakehouseId
            ,@IsIncremental
            ,@IsIncrementalColumn);
    END
    ELSE
    BEGIN
        UPDATE [integration].[LandingzoneEntity]
        SET [DataSourceId] = @DataSourceId
            ,[IsActive] = @IsActive
            ,[SourceSchema] = @SourceSchema
            ,[SourceName] = @SourceName
			,[SourceCustomSelect] = @SourceCustomSelect
            ,[FileName] = @FileName
            ,[FileType] = @FileType
            ,[FilePath] = @FilePath
            ,[LakehouseId] = @LakehouseId
            ,[IsIncremental] = @IsIncremental
            ,[IsIncrementalColumn] = @IsIncrementalColumn
        OUTPUT INSERTED.[LandingzoneEntityId] INTO @OutputTable
        WHERE [LandingzoneEntityId] = @LandingzoneEntityId;
    END

    SELECT LandingzoneEntityId FROM @OutputTable;
END

GO

