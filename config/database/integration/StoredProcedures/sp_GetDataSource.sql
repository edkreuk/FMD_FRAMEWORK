




    CREATE     PROCEDURE [integration].[sp_GetDataSource](
       @Name NVARCHAR(100)
    
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    declare @DataSourceId int
    BEGIN
        set @DataSourceId= ( select isnull(DataSourceId,0) as DataSourceId  from   [integration].[DataSource] where  [Name] = @Name)
    END

            SELECT @DataSourceId AS DataSourceId

GO

