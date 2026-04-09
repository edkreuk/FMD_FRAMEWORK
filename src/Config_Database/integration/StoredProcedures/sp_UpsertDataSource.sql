



    CREATE   PROCEDURE [integration].[sp_UpsertDataSource](
        @ConnectionId INT 
        ,@DataSourceId INT = 0
        ,@Name NVARCHAR(100)
        ,@Namespace VARCHAR(10)
        ,@Type VARCHAR(30)
        ,@Description NVARCHAR(200)
        ,@IsActive BIT = 1
    )
    WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @InternalConnectionId BIGINT;
    DECLARE @OutputTable TABLE (DataSourceId INT);

    SET @InternalConnectionId = (SELECT [C].[ConnectionId]
                                 FROM [integration].[Connection] [C]
                                 WHERE [C].ConnectionId = @ConnectionId);

    IF NOT EXISTS (SELECT 1
                   FROM [integration].[DataSource]
                   WHERE [DataSourceId] = @DataSourceId)
    BEGIN
        INSERT INTO [integration].[DataSource]
            ([Name]
            ,[Namespace]
            ,[ConnectionId]
            ,[Type]
            ,[Description]
            ,[IsActive])
        OUTPUT INSERTED.[DataSourceId] INTO @OutputTable
        VALUES (@Name
            ,@Namespace
            ,@InternalConnectionId
            ,@Type
            ,@Description
            ,@IsActive);
    END
    ELSE
    BEGIN
        UPDATE [integration].[DataSource]
        SET [Name] = @Name
            ,[ConnectionId] = @InternalConnectionId
            ,[Namespace] = @Namespace
            ,[Type] = @Type
            ,[Description] = @Description
            ,[IsActive] = @IsActive
        OUTPUT INSERTED.[DataSourceId] INTO @OutputTable
        WHERE [DataSourceId] = @DataSourceId;
    END

    SELECT DataSourceId FROM @OutputTable;
END

GO

